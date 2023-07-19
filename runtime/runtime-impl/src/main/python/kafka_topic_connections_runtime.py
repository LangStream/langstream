import logging
from confluent_kafka import Consumer, Producer


def apply_default_configuration(streaming_cluster, configs):
    if 'admin' in streaming_cluster:
        configs.update(streaming_cluster['admin'])
    configs['key.deserializer'] = 'org.apache.kafka.common.serialization.StringDeserializer'
    configs['value.deserializer'] = 'org.apache.kafka.common.serialization.StringDeserializer'
    configs['key.serializer'] = 'org.apache.kafka.common.serialization.StringSerializer'
    configs['value.serializer'] = 'org.apache.kafka.common.serialization.StringSerializer'


def create_consumer(streaming_cluster, configuration):
    configs = configuration.copy()
    apply_default_configuration(streaming_cluster, configs)
    topic = configs['topic']
    del configs['topic']
    return KafkaTopicConsumer(topic, configs)


def create_producer(streaming_cluster, configuration):
    configs = configuration.copy()
    apply_default_configuration(streaming_cluster, configs)
    topic = configs['topic']
    del configs['topic']
    return KafkaTopicProducer(topic, configs)


class KafkaTopicConsumer(object):

    def __init__(self, topic, configs):
        self.topic = topic
        self.configs = configs
        self.consumer = None

    def start(self):
        self.consumer = Consumer(self.configs)
        self.consumer.subscribe([self.topic])

    def close(self):
        if self.consumer is not None:
            self.consumer.close()

    def read(self):
        messages = self.consumer.poll(1.0)
        records = [KafkaRecord(message) for message in messages]

        logging.info(f"Received {len(records)} records from Kafka {records}")
        return records


class KafkaTopicProducer(object):

    def __init__(self, topic, configs):
        self.configs = configs
        self.topic = topic
        self.producer = None

    def start(self):
        self.producer = Producer(self.configs)

    def close(self):
        if self.producer is not None:
            self.producer.close()

    def write(self, records):
        for record in records:
            for header in record.headers():
                pass
            self.producer.produce(self.topic, record.value(), record.key(), None, result_handler, None, record.headers())


class KafkaRecord(object):

    def __init__(self, consumer_record):
        self.consumer_record = consumer_record

    def key(self):
        return self.consumer_record.key()

    def value(self):
        return self.consumer_record.value()

    def origin(self):
        return self.consumer_record.topic()

    def timestamp(self):
        return self.consumer_record.timestamp()

    def headers(self):
        return self.consumer_record.headers()
