import logging

from confluent_kafka import Consumer, Producer
from confluent_kafka.serialization import StringDeserializer, StringSerializer

STRING_DESERIALIZER = StringDeserializer()
STRING_SERIALIZER = StringSerializer()

from .kafka_serialization import STRING_SERIALIZER, DOUBLE_SERIALIZER, LONG_SERIALIZER, \
    BOOLEAN_SERIALIZER
from .record import Record


def apply_default_configuration(streaming_cluster, configs):
    if 'admin' in streaming_cluster['configuration']:
        configs.update(streaming_cluster['configuration']['admin'])


def create_source(agent_id, streaming_cluster, configuration):
    configs = configuration.copy()
    apply_default_configuration(streaming_cluster, configs)
    configs['enable.auto.commit'] = 'false'
    if 'group.id' not in configs:
        configs['group.id'] = 'sga-' + agent_id
    if 'auto.offset.reset' not in configs:
        configs['auto.offset.reset'] = 'earliest'
    if 'key.deserializer' not in configs:
        configs['key.deserializer'] = 'org.apache.kafka.common.serialization.StringDeserializer'
    if 'value.deserializer' not in configs:
        configs['value.deserializer'] = 'org.apache.kafka.common.serialization.StringDeserializer'
    return KafkaSource(configs)


def create_sink(_, streaming_cluster, configuration):
    configs = configuration.copy()
    apply_default_configuration(streaming_cluster, configs)
    if 'key.serializer' not in configs:
        configs['key.serializer'] = 'org.apache.kafka.common.serialization.StringSerializer'
    if 'value.serializer' not in configs:
        configs['value.serializer'] = 'org.apache.kafka.common.serialization.StringSerializer'
    return KafkaSink(configs)


class KafkaSource(object):
    def __init__(self, configs):
        self.configs = configs.copy()
        self.topic = self.configs.pop('topic')
        self.key_deserializer = self.configs.pop('key.deserializer')
        self.value_deserializer = self.configs.pop('value.deserializer')
        self.consumer = None

    def start(self):
        self.consumer = Consumer(self.configs)
        self.consumer.subscribe([self.topic])

    def close(self):
        if self.consumer:
            self.consumer.close()

    def read(self):
        message = self.consumer.poll(1.0)
        if message is None:
            return []
        if message.error():
            logging.error(f"Consumer error: {message.error()}")
            return []
        logging.info(f"Received message from Kafka {message}")
        return [KafkaRecord(message)]

    def commit(self):
        self.consumer.commit(asynchronous=False)


class KafkaSink(object):
    def __init__(self, configs):
        self.configs = configs.copy()
        self.topic = self.configs.pop('topic')
        self.key_serializer = self.configs.pop('key.serializer')
        self.value_serializer = self.configs.pop('value.serializer')
        self.producer = None

    def start(self):
        self.producer = Producer(self.configs)

    def write(self, records):
        for record in records:
            # TODO: handle send errors
            logging.info(f"Sending record {record}")
            headers = []
            if record.headers():
                for key, value in record.headers():
                    if type(value) == bytes:
                        headers.append((key, value))
                    elif type(value) == str:
                        headers.append((key, STRING_SERIALIZER(value)))
                    elif type(value) == float:
                        headers.append((key, DOUBLE_SERIALIZER(value)))
                    elif type(value) == int:
                        headers.append((key, LONG_SERIALIZER(value)))
                    elif type(value) == bool:
                        headers.append((key, BOOLEAN_SERIALIZER(value)))
                    else:
                        raise ValueError(f'Unsupported header type {type(value)} for header {(key, value)}')
            self.producer.produce(
                self.topic,
                value=STRING_SERIALIZER(record.value()),
                key=STRING_SERIALIZER(record.key()),
                headers=headers)
        self.producer.flush()


class KafkaRecord(Record):
    def __init__(self, consumer_record):
        super().__init__(
            STRING_DESERIALIZER(consumer_record.value()),
            key=STRING_DESERIALIZER(consumer_record.key()),
            origin=consumer_record.topic(),
            timestamp=consumer_record.timestamp(),
            headers=consumer_record.headers())
