/**
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.langstream.kafka.runner;

import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.topics.OffsetPerPartition;
import ai.langstream.api.runner.topics.TopicReadResult;
import ai.langstream.api.runner.topics.TopicReader;
import ai.langstream.api.runner.topics.TopicOffsetPosition;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

@Slf4j
class KafkaReaderWrapper implements TopicReader {

    static final ObjectMapper mapper = new ObjectMapper();
    private final Map<String, Object> configuration;
    private final String topicName;
    private final TopicOffsetPosition initialPosition;
    KafkaConsumer consumer;

    public KafkaReaderWrapper(Map<String, Object> configuration, String topicName, TopicOffsetPosition initialPosition) {
        this.configuration = configuration;
        this.topicName = topicName;
        this.initialPosition = initialPosition;
    }

    @Override
    public void start() {
        consumer = new KafkaConsumer(configuration);
        final List<TopicPartition> partitions = ((List<PartitionInfo>) consumer.partitionsFor(topicName))
                .stream()
                .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
                .collect(Collectors.toList());
        consumer.assign(partitions);
        if (initialPosition.position() == TopicOffsetPosition.Position.Latest) {
            consumer.seekToEnd(partitions);
        } else if (initialPosition.position() == TopicOffsetPosition.Position.Earliest) {
            consumer.seekToBeginning(partitions);
        } else {
            final OffsetPerPartition offsetPerPartition = parseOffset();
            for (TopicPartition partition : partitions) {
                final String offsetStr = offsetPerPartition.offsets().get(partition.partition() + "");
                if (offsetStr == null) {
                    log.info("No offset found for partition {}-{}, seeking to end", topicName, partition);
                    consumer.seekToEnd(List.of(partition));
                } else {
                    consumer.seek(partition, Long.parseLong(offsetStr));
                }
            }
        }

        // this MAY look like debug code, but it's not.
        // the "seek" operations are "lazy"
        // so we need to call "position" to actually trigger the seek
        // otherwise if a producer writes to the topic, we may lose the first message
        for (TopicPartition topicPartition : partitions) {
            long position = consumer.position(topicPartition);
            log.info("Current position for partition {} is {}", topicPartition, position);
        }

    }

    @SneakyThrows
    private OffsetPerPartition parseOffset() {
        return mapper.readValue(initialPosition.offset(), OffsetPerPartition.class);
    }

    @Override
    public void close() {
        if (consumer != null) {
            consumer.close();
        }
    }


    @Override
    public TopicReadResult read() {
        ConsumerRecords<?, ?> poll = consumer.poll(Duration.ofSeconds(5));
        List<Record> records = new ArrayList<>(poll.count());
        for (ConsumerRecord<?, ?> record : poll) {
            records.add(KafkaRecord.fromKafkaConsumerRecord(record));
        }
        final Set assignment = consumer.assignment();
        if (!records.isEmpty()) {
            log.info("Received {} records from Kafka topics {}: {}", records.size(), assignment, records);
        }
        Map<TopicPartition, Long> offsets = consumer.endOffsets(assignment);

        Map<String, String> partitions = new ConcurrentHashMap<>();
        for (Map.Entry<TopicPartition, Long> topicPartitionLongEntry : offsets.entrySet()) {
            final TopicPartition key = topicPartitionLongEntry.getKey();
            partitions.put(key.partition() + "", topicPartitionLongEntry.getValue() + "");
        }
        final OffsetPerPartition offsetPerPartition = new OffsetPerPartition(partitions);
        return new TopicReadResult() {
            @Override
            public List<Record> records() {
                return records;
            }

            @Override
            public OffsetPerPartition partitionsOffsets() {
                return offsetPerPartition;
            }
        };
    }
}
