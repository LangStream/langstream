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

from typing import List

import pytest
import waiting
import yaml
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.serialization import StringDeserializer, StringSerializer
from testcontainers.kafka import KafkaContainer

from langstream.internal import kafka_connection, runtime
from langstream.api import Record
from langstream.util import SimpleRecord, SingleRecordProcessor


def test_kafka_topic_connection():
    with KafkaContainer(image='confluentinc/cp-kafka:7.4.0') as container:
        input_topic = 'input-topic'
        output_topic = 'output-topic'
        bootstrap_server = container.get_bootstrap_server()

        config_yaml = f"""
        streamingCluster:
            type: kafka
            configuration:
                admin:
                    bootstrap.servers: {bootstrap_server}
        
        input:
            topic: {input_topic}
        
        output:
            topic: {output_topic}
              
        agent:
            applicationId: testApplicationId
            agentId: testAgentId
            configuration:
                className: tests.test_kafka_connection.TestSuccessProcessor
        """

        config = yaml.safe_load(config_yaml)

        producer = Producer({'bootstrap.servers': bootstrap_server})
        producer.produce(input_topic, StringSerializer()('verification message'), headers=[('prop-key', b'prop-value')])
        producer.flush()

        consumer = Consumer({
            'bootstrap.servers': bootstrap_server,
            'group.id': 'foo',
            'auto.offset.reset': 'earliest'
        })

        try:
            consumer.subscribe([output_topic])

            msg = None
            for i in range(10):
                runtime.run(config, max_loops=1)
                msg = consumer.poll(1.0)
                if msg and msg.error() is None:
                    break

            assert msg is not None
            assert StringDeserializer()(msg.value()) == 'verification message'
            assert msg.headers() == [
                ('prop-key', b'prop-value'),
                ('string', b'header-string'),
                ('bytes', b'header-bytes'),
                ('int', b'\x00\x00\x00\x00\x00\x00\x00\x2A'),
                ('float', b'\x40\x45\x00\x00\x00\x00\x00\x00'),
                ('boolean', b'\x01')
            ]
        finally:
            consumer.close()


def test_kafka_commit():
    with KafkaContainer(image='confluentinc/cp-kafka:7.4.0') as container:
        input_topic = 'input-topic'
        bootstrap_server = container.get_bootstrap_server()

        config_yaml = f"""
        streamingCluster:
            type: kafka
            configuration:
                admin:
                    bootstrap.servers: {bootstrap_server}
        """

        config = yaml.safe_load(config_yaml)
        source = kafka_connection.create_topic_consumer("id", config['streamingCluster'], {'topic': input_topic})
        source.start()

        producer = Producer({'bootstrap.servers': bootstrap_server})
        for _ in range(4):
            producer.produce(input_topic, b'message')
        producer.flush()

        records = [source.read()[0], source.read()[0]]
        source.commit(records)
        waiting.wait(lambda: source.consumer.committed([TopicPartition(input_topic, partition=0)])[0].offset == 1,
                     timeout_seconds=5, sleep_seconds=0.1)

        # Commit unordered fails
        source.read()
        with pytest.raises(Exception):
            source.commit([source.read()[0]])


def test_kafka_dlq():
    with KafkaContainer(image='confluentinc/cp-kafka:7.4.0') as container:
        input_topic = 'input-topic'
        output_topic = 'output-topic'
        dlq_topic = 'dlq-topic'
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
                className: tests.test_kafka_connection.TestFailingProcessor
            errorHandlerConfiguration:
                retries: 5
                onFailure: dead-letter
        """

        config = yaml.safe_load(config_yaml)

        producer = Producer({'bootstrap.servers': bootstrap_server})
        producer.produce(input_topic, StringSerializer()('verification message'))
        producer.flush()

        consumer = Consumer({
            'bootstrap.servers': bootstrap_server,
            'group.id': 'foo',
            'auto.offset.reset': 'earliest'
        })

        try:
            consumer.subscribe([dlq_topic])

            msg = None
            for i in range(15):
                runtime.run(config, max_loops=1)
                msg = consumer.poll(1.0)
                if msg and msg.error() is None:
                    break

            assert msg is not None
            assert StringDeserializer()(msg.value()) == 'verification message'
        finally:
            consumer.close()


class TestSuccessProcessor(SingleRecordProcessor):
    def process_record(self, record: Record) -> List[Record]:
        headers = record.headers().copy()
        headers.append(('string', 'header-string'))
        headers.append(('bytes', b'header-bytes'))
        headers.append(('int', 42))
        headers.append(('float', 42.0))
        headers.append(('boolean', True))
        return [SimpleRecord(record.value(), headers=headers)]


class TestFailingProcessor(SingleRecordProcessor):
    def process_record(self, record: Record) -> List[Record]:
        raise Exception('failed to process')
