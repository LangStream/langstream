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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

@Slf4j
public class KafkaConsumerWrapper implements TopicConsumer, ConsumerRebalanceListener {
    private final Map<String, Object> configuration;
    private final String topicName;
    private final AtomicInteger totalOut = new AtomicInteger();
    KafkaConsumer consumer;
    private boolean commitEverCalled;

    final AtomicInteger pendingCommits = new AtomicInteger(0);
    final AtomicReference<Throwable> commitFailure = new AtomicReference();

    @Getter
    private final Map<TopicPartition, TreeSet<Long>> uncommittedOffsets = new ConcurrentHashMap<>();

    private final Map<TopicPartition, OffsetAndMetadata> committed = new ConcurrentHashMap<>();

    public KafkaConsumerWrapper(Map<String, Object> configuration, String topicName) {
        this.configuration = configuration;
        this.topicName = topicName;
    }

    @Override
    public synchronized Object getNativeConsumer() {
        if (consumer == null) {
            throw new IllegalStateException("Consumer not started");
        }
        return consumer;
    }

    @Override
    public synchronized void start() {
        consumer = new KafkaConsumer(configuration);
        if (topicName != null) {
            log.info("Subscribing consumer to {}", topicName);
            consumer.subscribe(List.of(topicName), this);
        }
    }

    @Override
    public synchronized void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.info("Partitions revoked: {}", partitions);
        for (TopicPartition topicPartition : partitions) {
            OffsetAndMetadata offsetAndMetadata = committed.remove(topicPartition);
            if (offsetAndMetadata != null) {
                log.info(
                        "Current offset {} on partition {} (revoked)",
                        offsetAndMetadata.offset(),
                        topicPartition);
            }
            TreeSet<Long> remove = uncommittedOffsets.remove(topicPartition);
            if (remove != null && !remove.isEmpty()) {
                log.warn(
                        "There are uncommitted offsets {} on partition {} (revoked), this messages will be re-delivered",
                        remove,
                        topicPartition);
            }
        }
    }

    @Override
    public synchronized void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.info("Partitions assigned: {}", partitions);
        for (TopicPartition topicPartition : partitions) {
            OffsetAndMetadata offsetAndMetadata = consumer.committed(topicPartition);
            if (offsetAndMetadata != null) {
                log.info(
                        "Last committed offset for {} is {}",
                        topicPartition,
                        offsetAndMetadata.offset());
                committed.put(topicPartition, offsetAndMetadata);
            } else {
                log.info("Last committed offset for {} is null", topicPartition);
            }
        }
    }

    @Override
    public synchronized long getTotalOut() {
        return totalOut.get();
    }

    private synchronized KafkaConsumer getConsumer() {
        return consumer;
    }

    @Override
    public Map<String, Object> getInfo() {
        Map<String, Object> result = new HashMap<>();
        KafkaConsumer consumer = getConsumer();
        if (consumer != null) {
            Map<String, Object> committedOffsetsInfo = new HashMap<>();
            committed.forEach(
                    (topicPartition, offsetAndMetadata) -> {
                        committedOffsetsInfo.put(
                                topicPartition.topic() + "-" + topicPartition.partition(),
                                offsetAndMetadata.offset());
                    });
            result.put("committedOffsets", committedOffsetsInfo);

            Map<String, Object> uncommittedOffsetsInfo = new HashMap<>();

            uncommittedOffsets.forEach(
                    (topicPartition, offsets) -> {
                        uncommittedOffsetsInfo.put(
                                topicPartition.topic() + "-" + topicPartition.partition(),
                                offsets.size());
                    });
            result.put("uncommittedOffsets", uncommittedOffsetsInfo);

            result.put(
                    "kafkaConsumerMetrics",
                    KafkaMetricsUtils.metricsToMap(this.consumer.metrics()));
        }
        return result;
    }

    @Override
    public synchronized void close() {
        if (consumer != null) {
            if (topicName != null && commitEverCalled) {
                log.info("Committing offsets on {}: {}", topicName, committed);
                consumer.commitSync(committed);
            }
            int sum = uncommittedOffsets.values().stream().mapToInt(Set::size).sum();
            log.info(
                    "Closing consumer to {} with {} pending commits and {} uncommitted offsets: {} ",
                    topicName,
                    pendingCommits.get(),
                    sum,
                    uncommittedOffsets);
            consumer.close();
        }
    }

    @Override
    public synchronized List<Record> read() {
        if (commitFailure.get() != null) {
            throw new RuntimeException("latest commit failed", commitFailure.get());
        }
        KafkaConsumer consumer = getConsumer();
        ConsumerRecords<?, ?> poll = consumer.poll(Duration.ofSeconds(1));
        List<Record> result = new ArrayList<>(poll.count());
        for (ConsumerRecord<?, ?> record : poll) {
            result.add(KafkaRecord.fromKafkaConsumerRecord(record));
        }
        if (log.isDebugEnabled() && !result.isEmpty()) {
            log.debug("Received {} records from Kafka topics", result.size());
        }
        totalOut.addAndGet(result.size());
        return result;
    }

    /**
     * Commit the offsets of the records. This method may be called from different threads. Per each
     * partition we must keep track of the offsets that have been committed. But we can commit only
     * offsets sequentially, without gaps. It is possible that in case of out-of-order processing we
     * have to commit only a subset of the records. In case of rebalance or failure messages will be
     * re-delivered.
     *
     * @param records the records to commit, it is not strictly required from them to be in some
     *     order.
     */
    @Override
    public synchronized void commit(List<Record> records) {
        commitEverCalled = true;
        for (Record record : records) {
            KafkaRecord.KafkaConsumerOffsetProvider kafkaRecord =
                    (KafkaRecord.KafkaConsumerOffsetProvider) record;
            TopicPartition topicPartition = kafkaRecord.getTopicPartition();
            long offset = kafkaRecord.offset() + 1;
            OffsetAndMetadata offsetAndMetadata = committed.get(topicPartition);
            if (offsetAndMetadata == null) {
                offsetAndMetadata = consumer.committed(topicPartition);
                log.info(
                        "Current position on partition {} is {}",
                        topicPartition,
                        offsetAndMetadata);
                if (offsetAndMetadata != null) {
                    committed.put(topicPartition, offsetAndMetadata);
                }
            }
            long currentOffset = offsetAndMetadata == null ? 0 : offsetAndMetadata.offset();

            if (offset <= currentOffset) {
                throw new IllegalStateException(
                        ("Commit called with offset %s less than or equal to the currently committed offset %s "
                                        + "on partition %s")
                                .formatted(offset, currentOffset, topicPartition));
            }

            TreeSet<Long> offsetsForPartition =
                    uncommittedOffsets.computeIfAbsent(topicPartition, (key) -> new TreeSet<>());
            offsetsForPartition.add(offset);

            // advance the offset up the first gap
            long least = offsetsForPartition.first();

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
                if (log.isDebugEnabled()) {
                    log.debug(
                            "Committing offset {} on partition {} (record: {})",
                            offset,
                            topicPartition,
                            kafkaRecord);
                }
            }
            if (!offsetsForPartition.isEmpty()) {
                log.info(
                        "On partition {} there are {} uncommitted offsets",
                        topicPartition,
                        offsetsForPartition);
            }
        }

        pendingCommits.incrementAndGet();

        consumer.commitAsync(
                committed,
                (map, e) -> {
                    pendingCommits.decrementAndGet();
                    if (e != null) {
                        log.error("Error committing offsets on topic {}", topicName, e);
                        commitFailure.compareAndSet(null, e);
                    } else {
                        log.debug("Offsets committed: {}", map);
                    }
                });
    }
}
