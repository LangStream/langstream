import yaml
from confluent_kafka import Consumer, Producer
from confluent_kafka.serialization import StringDeserializer, StringSerializer
from testcontainers.kafka import KafkaContainer

from sga_runtime import sga_runtime


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
            applicationId: test
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
            if msg is None or msg.error():
                continue
            if msg:
                break

        assert msg is not None
        assert StringDeserializer()(msg.value()) == 'verification message'


class TestAgent(object):
    @staticmethod
    def process(records):
        return records
