import time

import pytest
import waiting
import yaml
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.serialization import StringDeserializer, StringSerializer
from testcontainers.kafka import KafkaContainer

from sga_runtime import sga_runtime, kafka_connection
from sga_runtime.api import Processor
from sga_runtime.simplerecord import SimpleRecord


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
                className: tests.test_kafka_connection.TestAgent
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
        consumer.subscribe([output_topic])

        msg = None
        for i in range(10):
            sga_runtime.run(config, 1)
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
        source = kafka_connection.create_source("id", config['streamingCluster'], {'topic': input_topic})
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


class TestAgent(Processor):
    def process(self, records):
        new_records = []
        for record in records:
            headers = record.headers().copy()
            headers.append(('string', 'header-string'))
            headers.append(('bytes', b'header-bytes'))
            headers.append(('int', 42))
            headers.append(('float', 42.0))
            headers.append(('boolean', True))
            new_records.append((record, [SimpleRecord(record.value(), headers=headers)]))
        return new_records
