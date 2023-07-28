package com.dastastax.oss.sga.kafka.runner;

import com.datastax.oss.sga.api.runner.code.Record;
import com.datastax.oss.sga.api.runner.topics.OffsetPerPartition;
import com.datastax.oss.sga.api.runner.topics.TopicReadResult;
import com.datastax.oss.sga.api.runner.topics.TopicReader;
import com.datastax.oss.sga.api.runner.topics.TopicOffsetPosition;
import com.fasterxml.jackson.core.JsonProcessingException;
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
        log.info("Received {} records from Kafka topics {}: {}", records.size(), assignment, records);
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
