#
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

import logging
from typing import List, Dict, Optional

from confluent_kafka import Consumer, Producer, Message, TopicPartition, KafkaException
from confluent_kafka.serialization import StringDeserializer

from .kafka_serialization import STRING_SERIALIZER, DOUBLE_SERIALIZER, LONG_SERIALIZER, \
    BOOLEAN_SERIALIZER
from .topic_connector import TopicConsumer, TopicProducer
from ..api import Sink, Record, Source, CommitCallback
from ..util import SimpleRecord

STRING_DESERIALIZER = StringDeserializer()


def apply_default_configuration(streaming_cluster, configs):
    if 'admin' in streaming_cluster['configuration']:
        configs.update(streaming_cluster['configuration']['admin'])


def create_topic_consumer(agent_id, streaming_cluster, configuration):
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
    return KafkaTopicConsumer(configs)


def create_topic_producer(_, streaming_cluster, configuration):
    configs = configuration.copy()
    apply_default_configuration(streaming_cluster, configs)
    if 'key.serializer' not in configs:
        configs['key.serializer'] = 'org.apache.kafka.common.serialization.StringSerializer'
    if 'value.serializer' not in configs:
        configs['value.serializer'] = 'org.apache.kafka.common.serialization.StringSerializer'
    return KafkaTopicProducer(configs)


def create_dlq_producer(agent_id, streaming_cluster, configuration):
    dlq_conf = configuration.get("deadLetterTopicProducer")
    if not dlq_conf:
        return None
    logging.info(f'Creating deadletter topic producer for agent {agent_id} using configuration {configuration}')
    return create_topic_producer(agent_id, streaming_cluster, dlq_conf)


class KafkaRecord(SimpleRecord):
    def __init__(self, message: Message):
        super().__init__(
            STRING_DESERIALIZER(message.value()),
            key=STRING_DESERIALIZER(message.key()),
            origin=message.topic(),
            timestamp=message.timestamp()[1],
            headers=message.headers())
        self._message: Message = message
        self._topic_partition: TopicPartition = TopicPartition(message.topic(), message.partition())

    def topic_partition(self) -> TopicPartition:
        return self._topic_partition

    def offset(self):
        return self._message.offset()


class KafkaTopicConsumer(TopicConsumer):
    def __init__(self, configs):
        self.configs = configs.copy()
        self.configs['on_commit'] = self.on_commit
        self.topic = self.configs.pop('topic')
        self.key_deserializer = self.configs.pop('key.deserializer')
        self.value_deserializer = self.configs.pop('value.deserializer')
        self.consumer: Optional[Consumer] = None
        self.committed: Dict[TopicPartition, int] = {}
        self.pending_commits = 0
        self.commit_failure = None

    def start(self):
        self.consumer = Consumer(self.configs)
        self.consumer.subscribe([self.topic])

    def close(self):
        logging.info(f'Closing consumer to {self.topic} with {self.pending_commits} pending commits')
        if self.consumer:
            self.consumer.close()

    def read(self) -> List[KafkaRecord]:
        if self.commit_failure:
            raise self.commit_failure
        message = self.consumer.poll(1.0)
        if message is None:
            return []
        if message.error():
            logging.error(f"Consumer error: {message.error()}")
            return []
        logging.info(f"Received message from Kafka {message}")
        return [KafkaRecord(message)]

    def commit(self, records: List[KafkaRecord]):
        for record in records:
            topic_partition = record.topic_partition()
            offset = record.offset()
            logging.info(f"Committing offset {offset} on partition {topic_partition} (record: {record})")
            if topic_partition in self.committed and offset != self.committed[topic_partition] + 1:
                raise RuntimeError(f'There is an hole in the commit sequence for partition {record}')
            self.committed[topic_partition] = offset
        offsets = [TopicPartition(topic_partition.topic, partition=topic_partition.partition, offset=offset)
                   for topic_partition, offset in self.committed.items()]
        self.pending_commits += 1
        self.consumer.commit(offsets=offsets, asynchronous=True)

    def on_commit(self, error, partitions):
        self.pending_commits -= 1
        if error:
            logging.error(f'Error committing offsets: {error}')
            if not self.commit_failure:
                self.commit_failure = KafkaException(error)
        else:
            logging.info(f'Offsets committed: {partitions}')


class KafkaTopicProducer(TopicProducer):
    def __init__(self, configs):
        self.configs = configs.copy()
        self.topic = self.configs.pop('topic')
        self.key_serializer = self.configs.pop('key.serializer')
        self.value_serializer = self.configs.pop('value.serializer')
        self.producer: Optional[Producer] = None
        self.commit_callback: Optional[CommitCallback] = None

    def start(self):
        self.producer = Producer(self.configs)

    def write(self, records: List[Record]):
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
