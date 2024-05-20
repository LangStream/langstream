/*
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
import ai.langstream.api.runner.topics.TopicOffsetPosition;
import ai.langstream.api.runner.topics.TopicReadResult;
import ai.langstream.api.runner.topics.TopicReader;
import ai.langstream.api.util.ClassloaderUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

@Slf4j
class KafkaReaderWrapper implements TopicReader {

    static final ObjectMapper mapper = new ObjectMapper();
    private final Map<String, Object> configuration;
    private final String topicName;
    private final TopicOffsetPosition initialPosition;
    KafkaConsumer<?, ?> consumer;

    public KafkaReaderWrapper(
            Map<String, Object> configuration,
            String topicName,
            TopicOffsetPosition initialPosition) {
        this.configuration = configuration;
        this.topicName = topicName;
        this.initialPosition = initialPosition;
    }

    @Override
    public void start() throws IOException {
        try (var context =
                ClassloaderUtils.withContextClassloader(this.getClass().getClassLoader())) {
            consumer = new KafkaConsumer<>(configuration);
        }
        final List<TopicPartition> partitions =
                consumer.partitionsFor(topicName).stream()
                        .map(
                                partitionInfo ->
                                        new TopicPartition(
                                                partitionInfo.topic(), partitionInfo.partition()))
                        .collect(Collectors.toList());
        consumer.assign(partitions);
        if (initialPosition.position() == TopicOffsetPosition.Position.Latest) {
            consumer.seekToEnd(partitions);
        } else if (initialPosition.position() == TopicOffsetPosition.Position.Earliest) {
            consumer.seekToBeginning(partitions);
        } else {
            final OffsetPerPartition offsetPerPartition = parseOffset();
            for (TopicPartition partition : partitions) {
                final String offsetStr =
                        offsetPerPartition.offsets().get(partition.partition() + "");
                if (offsetStr == null) {
                    log.info(
                            "No offset found for partition {}-{}, seeking to end",
                            topicName,
                            partition);
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

    private OffsetPerPartition parseOffset() throws IOException {
        return mapper.readValue(initialPosition.offset(), OffsetPerPartition.class);
    }

    @Override
    public void close() {
        if (consumer != null) {
            try {
                consumer.close();
            } catch (org.apache.kafka.common.errors.InterruptException e) {
                log.warn("Interrupted while closing Kafka consumer", e);
            }
        }
    }

    @Override
    public TopicReadResult read() throws JsonProcessingException {
        ConsumerRecords<?, ?> poll = consumer.poll(Duration.ofSeconds(5));
        List<Record> records = new ArrayList<>(poll.count());
        for (ConsumerRecord<?, ?> record : poll) {
            records.add(KafkaRecord.fromKafkaConsumerRecord(record));
        }
        final Set<TopicPartition> assignment = consumer.assignment();
        if (!records.isEmpty() && log.isDebugEnabled()) {
            log.debug("Received {} records from Kafka topics {}", records.size(), assignment);
        }
        Map<TopicPartition, Long> offsets = consumer.endOffsets(assignment);

        Map<String, String> partitions = new ConcurrentHashMap<>();
        for (Map.Entry<TopicPartition, Long> topicPartitionLongEntry : offsets.entrySet()) {
            final TopicPartition key = topicPartitionLongEntry.getKey();
            partitions.put(key.partition() + "", topicPartitionLongEntry.getValue() + "");
        }
        final OffsetPerPartition offsetPerPartition = new OffsetPerPartition(partitions);
        byte[] offset = mapper.writeValueAsBytes(offsetPerPartition);
        return new TopicReadResult() {
            @Override
            public List<Record> records() {
                return records;
            }

            @Override
            public byte[] offset() {
                return offset;
            }
        };
    }
}
