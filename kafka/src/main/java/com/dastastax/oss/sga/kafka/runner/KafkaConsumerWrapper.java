package com.dastastax.oss.sga.kafka.runner;

import com.datastax.oss.sga.api.runner.code.Record;
import com.datastax.oss.sga.api.runner.topics.TopicConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
class KafkaConsumerWrapper implements TopicConsumer {
    private final Map<String, Object> configuration;
    private final String topicName;
    KafkaConsumer consumer;

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
        consumer.subscribe(List.of(topicName));
    }

    @Override
    public void close() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Override
    public List<Record> read() {
        ConsumerRecords<?, ?> poll = consumer.poll(Duration.ofSeconds(1));
        List<Record> result = new ArrayList<>(poll.count());
        for (ConsumerRecord<?, ?> record : poll) {
            result.add(new KafkaConsumerRecord(record));
        }
        log.info("Received {} records from Kafka {}", result.size(), result);
        return result;
    }

    @Override
    public void commit(List<Record> records) {
        for (Record record :records) {
            KafkaConsumerRecord kafkaRecord = (KafkaConsumerRecord) record;
            TopicPartition topicPartition = kafkaRecord.getTopicPartition();
            long offset = kafkaRecord.offset();
            log.info("Committing offset {} on partition {} (record: {})", offset, topicPartition, kafkaRecord);
            committed.compute(topicPartition, (key, existing) -> {
               log.info("Committing on partition {}: previous offset {}, new offset {}", key, existing, offset);
               if (existing == null) {
                   return new OffsetAndMetadata(offset);
               } else if (existing.offset() != offset - 1) {
                   throw new IllegalStateException("There is an hole in the commit sequence for partition " + key);
               } else {
                   return new OffsetAndMetadata(Math.max(offset, existing.offset()));
               }
            });
        }
        consumer.commitAsync(committed, new OffsetCommitCallback() {
            @Override
            public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                if (e != null) {
                    log.error("Error committing offsets", e);
                } else {
                    log.info("Offsets committed: {}", map);
                }
            }
        });
    }
}
