package com.dastastax.oss.sga.kafka.runner;

import com.datastax.oss.sga.api.runner.code.Record;
import com.datastax.oss.sga.api.runner.topics.TopicConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
class KafkaConsumerWrapper implements TopicConsumer {
    private final Map<String, Object> configuration;
    private final String topicName;
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
        return result;
    }

    @Override
    public void commit(List<Record> records) {
        for (Record record :records) {
            KafkaRecord.KafkaConsumerOffsetProvider kafkaRecord = (KafkaRecord.KafkaConsumerOffsetProvider) record;
            TopicPartition topicPartition = kafkaRecord.getTopicPartition();
            long offset = kafkaRecord.offset();
            log.info("Committing offset {} on partition {} (record: {})", offset, topicPartition, kafkaRecord);
            committed.compute(topicPartition, (key, existing) -> {
                log.info("Committing on partition {}: previous offset {}, new offset {}", key, existing, offset);
                if (existing != null && offset != existing.offset() + 1) {
                    throw new IllegalStateException("There is an hole in the commit sequence for partition " + key);
                }
                return new OffsetAndMetadata(offset);
            });
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
