#
# Copyright DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import time
from typing import List
from uuid import UUID

import pytest
import waiting
import yaml
from confluent_kafka import Consumer, Producer, TopicPartition, KafkaException
from confluent_kafka.serialization import StringDeserializer, StringSerializer
from kafka import KafkaConsumer
from kafka.errors import KafkaError, UnrecognizedBrokerVersion, NoBrokersAvailable
from testcontainers.core.waiting_utils import wait_container_is_ready
from testcontainers.kafka import KafkaContainer

from langstream import Record, SimpleRecord, SingleRecordProcessor
from langstream_runtime import kafka_connection
from langstream_runtime import runtime

SECURITY_PROTOCOL = "SASL_PLAINTEXT"
SASL_MECHANISM = "PLAIN"
USERNAME = "admin"
PASSWORD = "admin-secret"
KAFKA_IMAGE = "confluentinc/cp-kafka:7.4.0"
INPUT_TOPIC = "input-topic"
OUTPUT_TOPIC = "output-topic"


def test_kafka_topic_connection():
    with SaslKafkaContainer().with_env(
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
        "BROKER:PLAINTEXT,PLAINTEXT:SASL_PLAINTEXT",
    ).with_env(
        "KAFKA_LISTENER_NAME_PLAINTEXT_SASL_ENABLED_MECHANISMS", SASL_MECHANISM
    ).with_env(
        "KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG",
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
        + f'username="{USERNAME}" '
        + f'password="{PASSWORD}" '
        + 'user_admin="admin-secret" '
        + 'user_producer="producer-secret" '
        + 'user_consumer="consumer-secret";',
    ).with_env(
        "KAFKA_SASL_JAAS_CONFIG",
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
        + f'username="{USERNAME}" '
        + f'password="{PASSWORD}";',
    ) as container:
        bootstrap_server = container.get_bootstrap_server()

        config_yaml = f"""
            streamingCluster:
                type: kafka
                configuration:
                    admin:
                        bootstrap.servers: {bootstrap_server}
                        security.protocol: {SECURITY_PROTOCOL}
                        sasl.jaas.config: "org.apache.kafka.common.security.plain.PlainLoginModule required username='{USERNAME}' password='{PASSWORD}';"
                        sasl.mechanism: {SASL_MECHANISM}
            input:
                topic: {INPUT_TOPIC}

            output:
                topic: {OUTPUT_TOPIC}

            agent:
                applicationId: testApplicationId
                agentId: testAgentId
                configuration:
                    className: langstream_runtime.tests.test_kafka_connection.TestSuccessProcessor
            """  # noqa: E501

        config = yaml.safe_load(config_yaml)

        producer = Producer(
            {
                "bootstrap.servers": bootstrap_server,
                "sasl.mechanism": SASL_MECHANISM,
                "security.protocol": SECURITY_PROTOCOL,
                "sasl.username": USERNAME,
                "sasl.password": PASSWORD,
            }
        )
        producer.produce(
            INPUT_TOPIC,
            StringSerializer()("verification message"),
            headers=[("prop-key", b"prop-value")],
        )
        producer.flush()

        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_server,
                "sasl.mechanism": SASL_MECHANISM,
                "security.protocol": SECURITY_PROTOCOL,
                "sasl.username": USERNAME,
                "sasl.password": PASSWORD,
                "group.id": "foo",
                "auto.offset.reset": "earliest",
            }
        )

        try:
            consumer.subscribe([OUTPUT_TOPIC])

            msg = None
            for i in range(10):
                runtime.run(config, max_loops=1)
                msg = consumer.poll(1.0)
                if msg and msg.error() is None:
                    break

            assert msg is not None
            assert StringDeserializer()(msg.value()) == "verification message"
            assert msg.headers() == [("prop-key", b"prop-value")]
        finally:
            consumer.close()


def test_kafka_commit():
    with KafkaContainer(image=KAFKA_IMAGE) as container:
        bootstrap_server = container.get_bootstrap_server()

        config_yaml = f"""
        streamingCluster:
            type: kafka
            configuration:
                admin:
                    bootstrap.servers: {bootstrap_server}
        """

        config = yaml.safe_load(config_yaml)
        source = kafka_connection.create_topic_consumer(
            "id", config["streamingCluster"], {"topic": INPUT_TOPIC}
        )
        source.start()

        producer = Producer({"bootstrap.servers": bootstrap_server})
        for i in range(10):
            producer.produce(INPUT_TOPIC, f"message {i}".encode())
        producer.flush()

        records = [source.read()[0], source.read()[0]]
        source.commit(records)

        topic_partition = TopicPartition(INPUT_TOPIC, partition=0)

        waiting.wait(
            lambda: source.consumer.committed([topic_partition])[0].offset == 2,
            timeout_seconds=5,
            sleep_seconds=0.1,
        )

        # Re-committing records fails
        with pytest.raises(RuntimeError):
            source.commit(records)

        committed_later = source.read()
        source.commit(source.read())

        # There's a hole in the offsets so the last commit offset is recorded in
        # the uncommitted offsets
        assert source.uncommitted[topic_partition] == {4}

        time.sleep(1)
        assert source.consumer.committed([topic_partition])[0].offset == 2
        source.commit(committed_later)

        waiting.wait(
            lambda: source.consumer.committed([topic_partition])[0].offset == 4,
            timeout_seconds=5,
            sleep_seconds=0.1,
        )

        source.close()

        # Create a new source with the same id, resuming from the committed offset
        source = kafka_connection.create_topic_consumer(
            "id", config["streamingCluster"], {"topic": INPUT_TOPIC}
        )
        source.start()

        # Check that we resume on the correct message
        assert source.read(5)[0].value() == "message 4"

        # on_assign should have been called
        assert source.committed[topic_partition] == 4

        source.close()

        # on_revoke should have been called
        assert topic_partition not in source.committed


def test_kafka_dlq():
    with KafkaContainer(image=KAFKA_IMAGE) as container:
        dlq_topic = "dlq-topic"
        bootstrap_server = container.get_bootstrap_server()

        config_yaml = f"""
        streamingCluster:
            type: kafka
            configuration:
                admin:
                    bootstrap.servers: {bootstrap_server}

        input:
            topic: {INPUT_TOPIC}
            deadLetterTopicProducer:
                topic: {dlq_topic}

        output:
            topic: {OUTPUT_TOPIC}

        agent:
            applicationId: testApplicationId
            agentId: testAgentId
            configuration:
                className: langstream_runtime.tests.test_kafka_connection.TestFailingProcessor
            errorHandlerConfiguration:
                retries: 5
                onFailure: dead-letter
        """  # noqa: E501

        config = yaml.safe_load(config_yaml)

        producer = Producer({"bootstrap.servers": bootstrap_server})
        producer.produce(INPUT_TOPIC, StringSerializer()("verification message"))
        producer.flush()

        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_server,
                "group.id": "foo",
                "auto.offset.reset": "earliest",
            }
        )

        try:
            consumer.subscribe([dlq_topic])

            msg = None
            for i in range(15):
                runtime.run(config, max_loops=1)
                msg = consumer.poll(1.0)
                if msg and msg.error() is None:
                    break

            assert msg is not None
            assert StringDeserializer()(msg.value()) == "verification message"
        finally:
            consumer.close()


def test_producer_error():
    config_yaml = """
            streamingCluster:
                type: kafka
                configuration:
                    admin:
                        bootstrap.servers: 127.0.0.1:1234
            """

    config = yaml.safe_load(config_yaml)
    sink = kafka_connection.create_topic_producer(
        "id",
        config["streamingCluster"],
        {"topic": OUTPUT_TOPIC, "delivery.timeout.ms": 1},
    )
    sink.start()
    with pytest.raises(KafkaException):
        sink.write([SimpleRecord("will fail")])


def test_serializers():
    with KafkaContainer(image=KAFKA_IMAGE) as container:
        bootstrap_server = container.get_bootstrap_server()

        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_server,
                "group.id": "foo",
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe([OUTPUT_TOPIC])

        config_yaml = f"""
                streamingCluster:
                    type: kafka
                    configuration:
                        admin:
                            bootstrap.servers: {bootstrap_server}
                """

        config = yaml.safe_load(config_yaml)

        for serializer, record_value, message_value in [
            ("StringSerializer", "test", b"test"),
            ("BooleanSerializer", True, b"\x01"),
            ("ShortSerializer", 42, b"\x00\x2A"),
            ("IntegerSerializer", 42, b"\x00\x00\x00\x2A"),
            ("LongSerializer", 42, b"\x00\x00\x00\x00\x00\x00\x00\x2A"),
            ("FloatSerializer", 42.0, b"\x42\x28\x00\x00"),
            ("DoubleSerializer", 42.0, b"\x40\x45\x00\x00\x00\x00\x00\x00"),
            (
                "UUIDSerializer",
                UUID("00010203-0405-0607-0809-0a0b0c0d0e0f"),
                b"00010203-0405-0607-0809-0a0b0c0d0e0f",
            ),
            ("ByteArraySerializer", b"test", b"test"),
            ("ByteArraySerializer", "test", b"test"),
            ("ByteArraySerializer", True, b"\x01"),
            ("ByteArraySerializer", 42, b"\x00\x00\x00\x00\x00\x00\x00\x2A"),
            ("ByteArraySerializer", 42.0, b"\x40\x45\x00\x00\x00\x00\x00\x00"),
            (
                "ByteArraySerializer",
                UUID("00010203-0405-0607-0809-0a0b0c0d0e0f"),
                b"00010203-0405-0607-0809-0a0b0c0d0e0f",
            ),
            ("ByteArraySerializer", {"a": "b", "c": 42.0}, b'{"a": "b", "c": 42.0}'),
            (
                "ByteArraySerializer",
                [{"a": "b"}, {"c": 42.0}],
                b'[{"a": "b"}, {"c": 42.0}]',
            ),
        ]:
            sink = kafka_connection.create_topic_producer(
                "id",
                config["streamingCluster"],
                {
                    "topic": OUTPUT_TOPIC,
                    "key.serializer": "org.apache.kafka.common.serialization."
                    + serializer,
                    "value.serializer": "org.apache.kafka.common.serialization."
                    + serializer,
                },
            )
            sink.start()
            sink.write(
                [
                    SimpleRecord(
                        record_value,
                        key=record_value,
                        headers=[("test-header", record_value)],
                    )
                ]
            )

            def safe_consume():
                message = consumer.poll(1)
                if message is None or message.error():
                    return False
                assert message.value() == message_value
                assert message.key() == message_value
                assert message.headers()[0][0] == "test-header"
                if serializer == "ByteArraySerializer":
                    assert message.headers()[0][1] == message_value
                return True

            waiting.wait(safe_consume, timeout_seconds=5, sleep_seconds=0.1)

            sink.close()


def test_consumer_deserializers():
    with KafkaContainer(image=KAFKA_IMAGE) as container:
        bootstrap_server = container.get_bootstrap_server()

        producer = Producer({"bootstrap.servers": bootstrap_server})

        config_yaml = f"""
                streamingCluster:
                    type: kafka
                    configuration:
                        admin:
                            bootstrap.servers: {bootstrap_server}
                """

        config = yaml.safe_load(config_yaml)

        for deserializer, record_value, message_value in [
            ("StringDeserializer", "test", b"test"),
            ("BooleanDeserializer", True, b"\x01"),
            ("ShortDeserializer", 42, b"\x00\x2A"),
            ("IntegerDeserializer", 42, b"\x00\x00\x00\x2A"),
            ("LongDeserializer", 42, b"\x00\x00\x00\x00\x00\x00\x00\x2A"),
            ("FloatDeserializer", 42.0, b"\x42\x28\x00\x00"),
            ("DoubleDeserializer", 42.0, b"\x40\x45\x00\x00\x00\x00\x00\x00"),
            ("ByteArrayDeserializer", b"test", b"test"),
            (
                "UUIDDeserializer",
                UUID("00010203-0405-0607-0809-0a0b0c0d0e0f"),
                b"00010203-0405-0607-0809-0a0b0c0d0e0f",
            ),
        ]:
            producer.produce(INPUT_TOPIC, message_value, key=message_value)
            producer.flush()
            source = kafka_connection.create_topic_consumer(
                "id",
                config["streamingCluster"],
                {
                    "topic": INPUT_TOPIC,
                    "key.deserializer": "org.apache.kafka.common.serialization."
                    + deserializer,
                    "value.deserializer": "org.apache.kafka.common.serialization."
                    + deserializer,
                },
            )
            source.start()
            record = source.read(timeout=5)[0]
            source.commit([record])

            assert record.value() == record_value
            assert record.key() == record_value

            source.close()


class TestSuccessProcessor(SingleRecordProcessor):
    def process_record(self, record: Record) -> List[Record]:
        headers = record.headers().copy()
        return [SimpleRecord(record.value(), headers=headers)]


class TestFailingProcessor(SingleRecordProcessor):
    def process_record(self, record: Record) -> List[Record]:
        raise Exception("failed to process")


class SaslKafkaContainer(KafkaContainer):
    """ "KafkaContainer with support for SASL in the waiting probe"""

    def __init__(self, **kwargs):
        super().__init__(KAFKA_IMAGE, **kwargs)

    @wait_container_is_ready(
        UnrecognizedBrokerVersion, NoBrokersAvailable, KafkaError, ValueError
    )
    def _connect(self):
        bootstrap_server = self.get_bootstrap_server()
        consumer = KafkaConsumer(
            group_id="test",
            bootstrap_servers=[bootstrap_server],
            security_protocol=SECURITY_PROTOCOL,
            sasl_mechanism=SASL_MECHANISM,
            sasl_plain_username=USERNAME,
            sasl_plain_password=PASSWORD,
        )
        if not consumer.bootstrap_connected():
            raise KafkaError("Unable to connect with kafka container!")
