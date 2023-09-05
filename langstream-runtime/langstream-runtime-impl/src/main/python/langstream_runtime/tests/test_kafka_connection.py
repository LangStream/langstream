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

import waiting
import yaml
from confluent_kafka import Consumer, Producer, TopicPartition
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
        input_topic = "input-topic"
        output_topic = "output-topic"
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
                topic: {input_topic}

            output:
                topic: {output_topic}

            agent:
                applicationId: testApplicationId
                agentId: testAgentId
                configuration:
                    className: langstream_runtime.tests.test_kafka_connection.TestSuccessProcessor
            """  # noqa

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
            input_topic,
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
            consumer.subscribe([output_topic])

            msg = None
            for i in range(10):
                runtime.run(config, max_loops=1)
                msg = consumer.poll(1.0)
                if msg and msg.error() is None:
                    break

            assert msg is not None
            assert StringDeserializer()(msg.value()) == "verification message"
            assert msg.headers() == [
                ("prop-key", b"prop-value"),
                ("string", b"header-string"),
                ("bytes", b"header-bytes"),
                ("int", b"\x00\x00\x00\x00\x00\x00\x00\x2A"),
                ("float", b"\x40\x45\x00\x00\x00\x00\x00\x00"),
                ("boolean", b"\x01"),
            ]
        finally:
            consumer.close()


def test_kafka_commit():
    with KafkaContainer(image="confluentinc/cp-kafka:7.4.0") as container:
        input_topic = "input-topic"
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
            "id", config["streamingCluster"], {"topic": input_topic}
        )
        source.start()

        producer = Producer({"bootstrap.servers": bootstrap_server})
        for _ in range(4):
            producer.produce(input_topic, b"message")
        producer.flush()

        records = [source.read()[0], source.read()[0]]
        source.commit(records)
        waiting.wait(
            lambda: source.consumer.committed(
                [TopicPartition(input_topic, partition=0)]
            )[0].offset
            == 1,
            timeout_seconds=5,
            sleep_seconds=0.1,
        )

        committed_later = source.read()
        source.commit(source.read())
        time.sleep(1)
        assert (
            source.consumer.committed([TopicPartition(input_topic, partition=0)])[
                0
            ].offset
            == 1
        )
        source.commit(committed_later)

        waiting.wait(
            lambda: source.consumer.committed(
                [TopicPartition(input_topic, partition=0)]
            )[0].offset
            == 3,
            timeout_seconds=5,
            sleep_seconds=0.1,
        )


def test_kafka_dlq():
    with KafkaContainer(image="confluentinc/cp-kafka:7.4.0") as container:
        input_topic = "input-topic"
        output_topic = "output-topic"
        dlq_topic = "dlq-topic"
        bootstrap_server = container.get_bootstrap_server()

        config_yaml = f"""
        streamingCluster:
            type: kafka
            configuration:
                admin:
                    bootstrap.servers: {bootstrap_server}

        input:
            topic: {input_topic}
            deadLetterTopicProducer:
                topic: {dlq_topic}

        output:
            topic: {output_topic}

        agent:
            applicationId: testApplicationId
            agentId: testAgentId
            configuration:
                className: langstream_runtime.tests.test_kafka_connection.TestFailingProcessor
            errorHandlerConfiguration:
                retries: 5
                onFailure: dead-letter
        """  # noqa

        config = yaml.safe_load(config_yaml)

        producer = Producer({"bootstrap.servers": bootstrap_server})
        producer.produce(input_topic, StringSerializer()("verification message"))
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


class TestSuccessProcessor(SingleRecordProcessor):
    def process_record(self, record: Record) -> List[Record]:
        headers = record.headers().copy()
        headers.append(("string", "header-string"))
        headers.append(("bytes", b"header-bytes"))
        headers.append(("int", 42))
        headers.append(("float", 42.0))
        headers.append(("boolean", True))
        return [SimpleRecord(record.value(), headers=headers)]


class TestFailingProcessor(SingleRecordProcessor):
    def process_record(self, record: Record) -> List[Record]:
        raise Exception("failed to process")


class SaslKafkaContainer(KafkaContainer):
    """ "KafkaContainer with support for SASL in the waiting probe"""

    def __init__(self, **kwargs):
        super().__init__("confluentinc/cp-kafka:7.4.0", **kwargs)

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
