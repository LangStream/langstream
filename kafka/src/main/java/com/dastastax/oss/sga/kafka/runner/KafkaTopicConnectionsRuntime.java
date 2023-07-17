package com.dastastax.oss.sga.kafka.runner;

import com.dastastax.oss.sga.kafka.runtime.KafkaClusterRuntimeConfiguration;
import com.dastastax.oss.sga.kafka.runtime.KafkaStreamingClusterRuntime;
import com.datastax.oss.sga.api.model.StreamingCluster;
import com.datastax.oss.sga.api.runner.code.Record;
import com.datastax.oss.sga.api.runner.topics.TopicConnectionsRuntime;
import com.datastax.oss.sga.api.runner.topics.TopicConsumer;
import com.datastax.oss.sga.api.runner.topics.TopicProducer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Slf4j
public class KafkaTopicConnectionsRuntime implements TopicConnectionsRuntime {
    @Override
    public TopicConsumer createConsumer(StreamingCluster streamingCluster, Map<String, Object> configuration) {

        Map<String, Object> copy = new HashMap<>(configuration);
        applyDefaultConfiguration(streamingCluster, copy);
        String topicName = (String) copy.remove("topic");

        return new TopicConsumer() {
            KafkaConsumer consumer;
            @Override
            public void start() {
                consumer = new KafkaConsumer(copy);
                consumer.subscribe(List.of(topicName));
            }

            @Override
            public Object getNativeConsumer() {
                return consumer;
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
                for (ConsumerRecord<?,?> record : poll) {
                    // TODO: how to get actual schemas?
                    result.add(new KafkaRecord(record.topic(),
                            record.partition(),
                            record.offset(),
                            Schema.OPTIONAL_STRING_SCHEMA,
                            record.key(),
                            Schema.OPTIONAL_STRING_SCHEMA,
                            record.value()));
                }
                log.info("Received {} records from Kafka {}", result.size(), result);
                return result;
            }
        };
    }

    private static void applyDefaultConfiguration(StreamingCluster streamingCluster, Map<String, Object> copy) {
        KafkaClusterRuntimeConfiguration configuration = KafkaStreamingClusterRuntime.getKafkaClusterRuntimeConfiguration(streamingCluster);
        copy.putAll(configuration.getAdmin());

        // consumer
        copy.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        copy.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        copy.computeIfAbsent("group.id", key -> "sga");
        copy.computeIfAbsent("auto.offset.reset", key -> "earliest");

        // producer
        copy.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        copy.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }

    public record KafkaRecord (String topic, int kafkaPartition,
                               TopicPartition topicPartition,
                                       long kafkaOffset,
                                       Schema keySchema, Object key,
                                       Schema valueSchema, Object value,
                                       Long timestamp, TimestampType timestampType,
                                       Iterable<Header> headers) implements Record {

        public KafkaRecord {
            Objects.requireNonNull(topic);
            Objects.requireNonNull(kafkaPartition);

            // topicPartition is to avoid repeated creation of new TopicPartition for each record later
            if (!topicPartition.topic().equals(topic) || topicPartition.partition() != kafkaPartition) {
                throw new IllegalArgumentException("topicPartition is inconsistent with provided topic and partition");
            }
        }

        public KafkaRecord(String topic, int kafkaPartition,
                           long kafkaOffset,
                           Schema keySchema, Object key,
                           Schema valueSchema, Object value) {
            this(topic, kafkaPartition, new TopicPartition(topic, kafkaPartition), kafkaOffset,
                    keySchema, key, valueSchema, value,
                    null, TimestampType.NO_TIMESTAMP_TYPE, null);
        }

        public KafkaRecord(String topic, int kafkaPartition,
                           long kafkaOffset,
                           Schema keySchema, Object key,
                           Schema valueSchema, Object value,
                           Long timestamp, TimestampType timestampType,
                           Iterable<Header> headers) {
            this(topic, kafkaPartition, new TopicPartition(topic, kafkaPartition), kafkaOffset,
                    keySchema, key, valueSchema, value,
                    timestamp, timestampType, headers);
        }
    }

    @Override
    public TopicProducer createProducer(StreamingCluster streamingCluster, Map<String, Object> configuration) {
        Map<String, Object> copy = new HashMap<>(configuration);
        applyDefaultConfiguration(streamingCluster, copy);
        String topicName = (String) copy.remove("topic");

        return new TopicProducer() {

            KafkaProducer producer;

            @Override
            public void start() {
                producer = new KafkaProducer(copy);
            }

            @Override
            public void close() {
                if (producer != null) {
                    producer.close();
                }
            }

            @Override
            public java.lang.Object getNativeProducer() {
                return producer;
            }

            @Override
            @SneakyThrows
            public void write(List<Record> records) {
                for (Record r : records) {
                    KafkaRecord kafkaRecord = (KafkaRecord) r;
                    producer.send(new ProducerRecord(topicName, kafkaRecord.key, kafkaRecord.value)).get();
                }
            }
        };
    }
}
