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
import ai.langstream.api.runner.topics.TopicConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
class KafkaConsumerWrapper implements TopicConsumer {
    private final Map<String, Object> configuration;
    private final String topicName;
    private final AtomicInteger totalOut = new AtomicInteger();
    KafkaConsumer consumer;

    AtomicInteger pendingCommits = new AtomicInteger(0);
    AtomicReference<Throwable> commitFailure = new AtomicReference();

    private final Map<TopicPartition, OffsetAndMetadata> committed = new ConcurrentHashMap<>();

    public KafkaConsumerWrapper(Map<String, Object> configuration, String topicName) {
        this.configuration = configuration;
        this.topicName = topicName;
    }

    @Override
    public Object getNativeConsumer() {
        if (consumer == null) {
            throw new IllegalStateException("Consumer not started");
        }
        return consumer;
    }

    @Override
    public void start() {
        consumer = new KafkaConsumer(configuration);
        if (topicName != null) {
            consumer.subscribe(List.of(topicName));
        }
    }

    @Override
    public long getTotalOut() {
        return totalOut.get();
    }

    @Override
    public Map<String, Object> getInfo() {
        Map<String, Object> result = new HashMap<>();
        if (consumer != null) {
            result.put("kafkaConsumerMetrics",
                    KafkaMetricsUtils.metricsToMap(consumer.metrics()));
        }
        return result;
    }

    @Override
    public void close() {
        log.info("Closing consumer to {} with {} pending commits", topicName, pendingCommits.get());

        if (consumer != null) {
            consumer.close();
        }
    }

    @Override
    public List<Record> read() {
        if (commitFailure.get() != null) {
            throw new RuntimeException("latest commit failed", commitFailure.get());
        }
        ConsumerRecords<?, ?> poll = consumer.poll(Duration.ofSeconds(1));
        List<Record> result = new ArrayList<>(poll.count());
        for (ConsumerRecord<?, ?> record : poll) {
            result.add(KafkaRecord.fromKafkaConsumerRecord(record));
        }
        if (!result.isEmpty()) {
            log.info("Received {} records from Kafka topics {}: {}", result.size(), consumer.assignment(), result);
        }
        totalOut.addAndGet(result.size());
        return result;
    }


    private Map<TopicPartition, TreeSet<Long>> committedOffsets = new HashMap<>();

    @Override
    public void commit(List<Record> records) {
        for (Record record :records) {
            KafkaRecord.KafkaConsumerOffsetProvider kafkaRecord = (KafkaRecord.KafkaConsumerOffsetProvider) record;
            TopicPartition topicPartition = kafkaRecord.getTopicPartition();
            long offset = kafkaRecord.offset();
            TreeSet<Long> offsetsForPartition = committedOffsets.computeIfAbsent(topicPartition, (key) -> {
                return new TreeSet<>();
            });
            offsetsForPartition.add(offset);
            OffsetAndMetadata offsetAndMetadata = committed.get(topicPartition);
            if (offsetAndMetadata == null) {
                offsetAndMetadata = consumer.committed(topicPartition);
                log.info("Current position on partition {} is {}", topicPartition, offsetAndMetadata);
                if (offsetAndMetadata != null) {
                    committed.put(topicPartition, offsetAndMetadata);
                }
            }

            long least = offsetsForPartition.first();
            long currentOffset = offsetAndMetadata == null ? -1 : offsetAndMetadata.offset();
            while (least == currentOffset + 1) {
                offsetsForPartition.remove(least);
                offsetAndMetadata = new OffsetAndMetadata(least);
                currentOffset = least;
                if (offsetsForPartition.isEmpty()) {
                    break;
                }
                least = offsetsForPartition.first();
            }
            if (offsetAndMetadata != null) {
                committed.put(topicPartition, offsetAndMetadata);
                log.info("Committing offset {} on partition {} (record: {})", offset, topicPartition, kafkaRecord);
            }
        }

        pendingCommits.incrementAndGet();
        consumer.commitAsync(committed, (map, e) -> {
            pendingCommits.decrementAndGet();
            if (e != null) {
                log.error("Error committing offsets", e);
                commitFailure.compareAndSet(null, e);
            } else {
                log.info("Offsets committed: {}", map);
            }
        });
    }
}
