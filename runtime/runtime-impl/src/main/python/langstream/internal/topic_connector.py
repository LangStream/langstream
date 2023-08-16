import logging
from abc import ABC, abstractmethod
from typing import List

from langstream.api import Source, Record, Sink, CommitCallback


class TopicConsumer(object):

    def start(self):
        pass

    def close(self):
        pass

    def read(self) -> List[Record]:
        return []

    def commit(self, records: List[Record]):
        pass


class TopicProducer(object):

    def start(self):
        pass

    def close(self):
        pass

    def write(self, records: List[Record]):
        pass


class TopicConsumerSource(Source):

    def __init__(self, consumer: TopicConsumer):
        self.consumer = consumer

    def read(self) -> List[Record]:
        return self.consumer.read()

    def commit(self, records: List[Record]):
        self.consumer.commit(records)

    def start(self):
        logging.info(f'Starting consumer {self.consumer}')
        self.consumer.start()

    def close(self):
        logging.info(f'Closing consumer {self.consumer}')
        self.consumer.close()

    def __str__(self):
        return f'TopicConsumerSource{{consumer={self.consumer}}}'


class TopicConsumerWithDLQSource(TopicConsumerSource):

    def __init__(self, consumer: TopicConsumer, dlq_producer: TopicProducer):
        super().__init__(consumer)
        self.dlq_producer = dlq_producer

    def start(self):
        super().start()
        self.dlq_producer.start()

    def close(self):
        super().close()
        self.dlq_producer.close()

    def permanent_failure(self, record: Record, error: Exception):
        logging.error(f'Sending record to DLQ: {record}')
        self.dlq_producer.write([record])


class TopicProducerSink(Sink):

    def __init__(self, producer: TopicProducer):
        self.producer = producer
        self.commit_callback = None

    def start(self):
        logging.info(f'Starting producer {self.producer}')
        self.producer.start()

    def close(self):
        logging.info(f'Closing producer {self.producer}')
        self.producer.close()

    def write(self, records: List[Record]):
        self.producer.write(records)
        self.commit_callback.commit(records)

    def set_commit_callback(self, commit_callback: CommitCallback):
        self.commit_callback = commit_callback

    def __str__(self):
        return f'TopicProducerSink{{producer={self.producer}}}'


