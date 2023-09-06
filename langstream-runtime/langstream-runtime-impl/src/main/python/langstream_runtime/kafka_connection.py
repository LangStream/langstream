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
import re
import threading
from typing import List, Dict, Optional, Any

from confluent_kafka import Consumer, Producer, Message, TopicPartition, KafkaException
from confluent_kafka.serialization import StringDeserializer
from sortedcontainers import SortedSet

from langstream import Record, CommitCallback, SimpleRecord
from .kafka_serialization import (
    STRING_SERIALIZER,
    BOOLEAN_SERIALIZER,
    SHORT_SERIALIZER,
    INTEGER_SERIALIZER,
    LONG_SERIALIZER,
    FLOAT_SERIALIZER,
    DOUBLE_SERIALIZER,
    BYTEARRAY_SERIALIZER,
)
from .topic_connector import TopicConsumer, TopicProducer

LOG = logging.getLogger(__name__)
STRING_DESERIALIZER = StringDeserializer()

SERIALIZERS = {
    "org.apache.kafka.common.serialization.StringSerializer": STRING_SERIALIZER,
    "org.apache.kafka.common.serialization.BooleanSerializer": BOOLEAN_SERIALIZER,
    "org.apache.kafka.common.serialization.ShortSerializer": SHORT_SERIALIZER,
    "org.apache.kafka.common.serialization.IntegerSerializer": INTEGER_SERIALIZER,
    "org.apache.kafka.common.serialization.LongSerializer": LONG_SERIALIZER,
    "org.apache.kafka.common.serialization.FloatSerializer": FLOAT_SERIALIZER,
    "org.apache.kafka.common.serialization.DoubleSerializer": DOUBLE_SERIALIZER,
    "org.apache.kafka.common.serialization.ByteArraySerializer": BYTEARRAY_SERIALIZER,
}


def apply_default_configuration(streaming_cluster, configs):
    if "admin" in streaming_cluster["configuration"]:
        configs.update(streaming_cluster["configuration"]["admin"])

    # Compatibility with JAAS conf
    if "sasl.jaas.config" in configs:
        for prop in ["username", "password"]:
            if "sasl." + prop not in configs:
                prop_value = extract_jaas_property(prop, configs["sasl.jaas.config"])
                if prop_value:
                    configs["sasl." + prop] = prop_value
        del configs["sasl.jaas.config"]


def create_topic_consumer(agent_id, streaming_cluster, configuration):
    configs = configuration.copy()
    configs.pop("deadLetterTopicProducer", None)
    apply_default_configuration(streaming_cluster, configs)
    configs["enable.auto.commit"] = "false"
    if "group.id" not in configs:
        configs["group.id"] = "langstream-" + agent_id
    if "auto.offset.reset" not in configs:
        configs["auto.offset.reset"] = "earliest"
    if "key.deserializer" not in configs:
        configs[
            "key.deserializer"
        ] = "org.apache.kafka.common.serialization.StringDeserializer"
    if "value.deserializer" not in configs:
        configs[
            "value.deserializer"
        ] = "org.apache.kafka.common.serialization.StringDeserializer"
    return KafkaTopicConsumer(configs)


def create_topic_producer(_, streaming_cluster, configuration):
    configs = configuration.copy()
    apply_default_configuration(streaming_cluster, configs)
    if "key.serializer" not in configs:
        configs[
            "key.serializer"
        ] = "org.apache.kafka.common.serialization.StringSerializer"
    if "value.serializer" not in configs:
        configs[
            "value.serializer"
        ] = "org.apache.kafka.common.serialization.StringSerializer"
    return KafkaTopicProducer(configs)


def create_dlq_producer(agent_id, streaming_cluster, configuration):
    dlq_conf = configuration.get("deadLetterTopicProducer")
    if not dlq_conf:
        return None
    LOG.info(
        f"Creating dead-letter topic producer for agent {agent_id} using configuration "
        f"{configuration}"
    )
    return create_topic_producer(agent_id, streaming_cluster, dlq_conf)


def extract_jaas_property(prop, jaas_entry):
    re_pattern = re.compile(prop + r'\s*=\s*(?:"([^"]*)"|\'([^\']*)\'|(\S+))')
    re_match = re_pattern.search(jaas_entry)
    if re_match:
        return re_match.group(1) or re_match.group(2) or re_match.group(3)
    else:
        return None


class KafkaRecord(SimpleRecord):
    def __init__(self, message: Message):
        super().__init__(
            STRING_DESERIALIZER(message.value()),
            key=STRING_DESERIALIZER(message.key()),
            origin=message.topic(),
            timestamp=message.timestamp()[1],
            headers=message.headers(),
        )
        self._message: Message = message
        self._topic_partition: TopicPartition = TopicPartition(
            message.topic(), message.partition()
        )

    def topic_partition(self) -> TopicPartition:
        return self._topic_partition

    def offset(self):
        return self._message.offset()


class KafkaTopicConsumer(TopicConsumer):
    def __init__(self, configs):
        self.configs = configs.copy()
        self.configs["on_commit"] = self.on_commit
        self.topic = self.configs.pop("topic")
        self.key_deserializer = self.configs.pop("key.deserializer")
        self.value_deserializer = self.configs.pop("value.deserializer")
        self.consumer: Optional[Consumer] = None
        self.committed: Dict[TopicPartition, int] = {}
        self.uncommitted: Dict[TopicPartition, SortedSet[int]] = {}
        self.pending_commits = 0
        self.commit_failure = None
        self.lock = threading.RLock()
        self.commit_ever_called = False

    def start(self):
        self.consumer = Consumer(self.configs, logger=LOG)
        LOG.info(f"Subscribing consumer to {self.topic}")
        self.consumer.subscribe(
            [self.topic], on_assign=self.on_assign, on_revoke=self.on_revoke
        )

    def close(self):
        with self.lock:
            if self.consumer:
                LOG.info(
                    f"Closing consumer to {self.topic} with {self.pending_commits} "
                    f"pending commits and {len(self.uncommitted)} uncommitted "
                    f"offsets: {self.uncommitted} "
                )
                if self.commit_ever_called:
                    offsets = [
                        TopicPartition(
                            topic_partition.topic,
                            partition=topic_partition.partition,
                            offset=offset,
                        )
                        for topic_partition, offset in self.committed.items()
                    ]
                    self.consumer.commit(offsets=offsets, asynchronous=False)
                self.consumer.close()

    def read(self) -> List[KafkaRecord]:
        if self.commit_failure:
            raise self.commit_failure
        message = self.consumer.poll(1.0)
        if message is None:
            return []
        if message.error():
            LOG.error(f"Consumer error: {message.error()}")
            return []
        LOG.debug(
            f"Received message from Kafka topics {self.consumer.assignment()}:"
            f" {message}"
        )
        return [KafkaRecord(message)]

    def commit(self, records: List[KafkaRecord]):
        """Commit the offsets of the records.
        This method is thread-safe.
        Per each partition we must keep track of the offsets that have been committed.
        But we can commit only offsets sequentially, without gaps.
        It is possible that in case of out-of-order processing we have to commit only a
        subset of the records.
        In case of rebalance or failure, messages will be re-delivered.

        :param records: the records to commit, it is not strictly required from them to
        be in some order.
        """

        if len(records) == 0:
            return

        with self.lock:
            self.commit_ever_called = True
            for record in records:
                topic_partition = record.topic_partition()

                current_offset = self.committed.get(topic_partition)
                if current_offset is None:
                    committed_tp_offset = self.consumer.committed([topic_partition])[0]
                    if committed_tp_offset.error:
                        raise KafkaException(committed_tp_offset.error)
                    current_offset = committed_tp_offset.offset
                    LOG.info(
                        f"Current position on partition {topic_partition} is "
                        f"{current_offset}"
                    )
                    if current_offset < 0:
                        current_offset = 0

                offset = record.offset() + 1

                if offset <= current_offset:
                    raise RuntimeError(
                        f"Commit called with offset {offset} less than the currently "
                        f"committed offset{current_offset}."
                    )

                offsets_for_partition = self.uncommitted.setdefault(
                    topic_partition, SortedSet()
                )
                offsets_for_partition.add(offset)

                least = offsets_for_partition[0]
                # advance the offset up to the first gap
                while least == current_offset + 1:
                    current_offset = offsets_for_partition.pop(0)
                    if len(offsets_for_partition) == 0:
                        break
                    least = offsets_for_partition[0]

                if current_offset > 0:
                    self.committed[topic_partition] = current_offset

            offsets = [
                TopicPartition(
                    topic_partition.topic,
                    partition=topic_partition.partition,
                    offset=offset,
                )
                for topic_partition, offset in self.committed.items()
            ]
            self.pending_commits += 1
            self.consumer.commit(offsets=offsets, asynchronous=True)

    def on_commit(self, error, partitions):
        with self.lock:
            self.pending_commits -= 1
        if error:
            LOG.error(f"Error committing offsets on topic {self.topic}: {error}")
            if not self.commit_failure:
                self.commit_failure = KafkaException(error)
        else:
            LOG.debug(f"Offsets committed: {partitions}")

    def on_assign(self, consumer: Consumer, partitions: List[TopicPartition]):
        with self.lock:
            LOG.info(f"Partitions assigned: {partitions}")
            for partition in partitions:
                offset = consumer.committed([partition])[0].offset
                LOG.info(f"Last committed offset for {partition} is {offset}")
                if offset >= 0:
                    self.committed[partition] = offset

    def on_revoke(self, _, partitions: List[TopicPartition]):
        with self.lock:
            LOG.info(f"Partitions revoked: {partitions}")
            for partition in partitions:
                if partition in self.committed:
                    offset = self.committed.pop(partition)
                    LOG.info(
                        f"Current offset {offset} on partition {partition} (revoked)"
                    )
                if partition in self.uncommitted:
                    offsets = self.uncommitted.pop(partition)
                    if len(offsets) > 0:
                        LOG.warning(
                            f"There are uncommitted offsets {offsets} on partition "
                            f"{partition} (revoked), these messages will be "
                            f"re-delivered"
                        )

    def get_native_consumer(self) -> Any:
        return self.consumer

    def get_info(self) -> Dict[str, Any]:
        with self.lock:
            return {
                "committedOffsets": {
                    f"{tp.topic}-{tp.partition}": offset
                    for tp, offset in self.committed.items()
                },
                "uncommittedOffsets": {
                    f"{tp.topic}-{tp.partition}": len(offsets)
                    for tp, offsets in self.uncommitted.items()
                },
            }


class KafkaTopicProducer(TopicProducer):
    def __init__(self, configs):
        self.configs = configs.copy()
        self.topic = self.configs.pop("topic")
        self.key_serializer = SERIALIZERS[self.configs.pop("key.serializer")]
        self.value_serializer = SERIALIZERS[self.configs.pop("value.serializer")]
        self.producer: Optional[Producer] = None
        self.commit_callback: Optional[CommitCallback] = None
        self.delivery_failure: Optional[Exception] = None

    def start(self):
        self.producer = Producer(self.configs, logger=LOG)

    def write(self, records: List[Record]):
        for record in records:
            if self.delivery_failure:
                raise self.delivery_failure
            LOG.info(f"Sending record {record}")
            headers = []
            if record.headers():
                for key, value in record.headers():
                    if isinstance(value, bytes):
                        headers.append((key, value))
                    elif isinstance(value, str):
                        headers.append((key, STRING_SERIALIZER(value)))
                    elif isinstance(value, bool):
                        headers.append((key, BOOLEAN_SERIALIZER(value)))
                    elif isinstance(value, int):
                        headers.append((key, LONG_SERIALIZER(value)))
                    elif isinstance(value, float):
                        headers.append((key, DOUBLE_SERIALIZER(value)))
                    else:
                        raise ValueError(
                            f"Unsupported header type {type(value)} for header "
                            f"{(key, value)}"
                        )
            self.producer.produce(
                self.topic,
                value=self.value_serializer(record.value()),
                key=self.key_serializer(record.key()),
                headers=headers,
                on_delivery=self.on_delivery,
            )
        self.producer.flush()
        if self.delivery_failure:
            raise self.delivery_failure

    def get_native_producer(self) -> Any:
        return self.producer

    def on_delivery(self, error, _):
        if error and not self.delivery_failure:
            self.delivery_failure = KafkaException(error)
