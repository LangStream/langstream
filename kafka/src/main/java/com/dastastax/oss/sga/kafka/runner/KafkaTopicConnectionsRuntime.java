package com.dastastax.oss.sga.kafka.runner;

import com.dastastax.oss.sga.kafka.runtime.KafkaClusterRuntimeConfiguration;
import com.dastastax.oss.sga.kafka.runtime.KafkaStreamingClusterRuntime;
import com.datastax.oss.sga.api.model.StreamingCluster;
import com.datastax.oss.sga.api.runner.code.Header;
import com.datastax.oss.sga.api.runner.code.Record;
import com.datastax.oss.sga.api.runner.topics.TopicConnectionsRuntime;
import com.datastax.oss.sga.api.runner.topics.TopicConsumer;
import com.datastax.oss.sga.api.runner.topics.TopicProducer;
import java.util.UUID;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.BooleanSerializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.ShortSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.UUIDSerializer;
import org.apache.kafka.connect.data.Schema;

@Slf4j
public class KafkaTopicConnectionsRuntime implements TopicConnectionsRuntime {

    private static final Map<Class<?>, Serializer<?>> SERIALIZERS = Map.of(
        String.class, new StringSerializer(),
        Boolean.class, new BooleanSerializer(),
        Short.class, new ShortSerializer(),
        Integer.class, new IntegerSerializer(),
        Long.class, new LongSerializer(),
        Float.class, new FloatSerializer(),
        Double.class, new DoubleSerializer(),
        byte[].class, new ByteArraySerializer(),
        UUID.class, new UUIDSerializer()
    );

    @Override
    public TopicConsumer createConsumer(String agentId, StreamingCluster streamingCluster, Map<String, Object> configuration) {

        Map<String, Object> copy = new HashMap<>(configuration);
        applyDefaultConfiguration(agentId, streamingCluster, copy);
        String topicName = (String) copy.remove("topic");

        return new KafkaConsumerWrapper(copy, topicName);
    }

    private static void applyDefaultConfiguration(String agentId, StreamingCluster streamingCluster, Map<String, Object> copy) {
        KafkaClusterRuntimeConfiguration configuration = KafkaStreamingClusterRuntime.getKafkaClusterRuntimeConfiguration(streamingCluster);
        copy.putAll(configuration.getAdmin());

        // consumer
        copy.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        copy.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        copy.put("enable.auto.commit", "false");
        copy.computeIfAbsent("group.id", key -> "sga-" + agentId);
        copy.putIfAbsent("auto.offset.reset", "earliest");

        // producer
        copy.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        copy.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    }

    @ToString
    public static class KafkaRecord implements Record {
        private final ConsumerRecord<?, ?> record;
        private final List<Header> headers = new ArrayList<>();
        private final TopicPartition topicPartition;

        public KafkaRecord(ConsumerRecord<?, ?> record) {
            this.record = record;
            this.topicPartition = new TopicPartition(record.topic(), record.partition());
            for (org.apache.kafka.common.header.Header header : record.headers()) {
                headers.add(new KafkaHeader(header));
            }
        }

        public TopicPartition getTopicPartition() {
            return topicPartition;
        }

        @Override
        public Object key() {
            return record.key();
        }

        @Override
        public Object value() {
            return record.value();
        }

        public int estimateRecordSize() {
            return record.serializedKeySize() + record.serializedValueSize();
        }

        public org.apache.kafka.connect.data.Schema keySchema() {
            return null;
        }

        public org.apache.kafka.connect.data.Schema valueSchema() {
            return null;
        }

        @Override
        public String origin() {
            return record.topic();
        }

        public int partition() {
            return record.partition();
        }

        public long offset() {
            return record.offset();
        }

        @Override
        public Long timestamp() {
            return record.timestamp();
        }

        public TimestampType timestampType() {
            return record.timestampType();
        }

        @Override
        public List<Header> headers() {
            return headers;
        }

    }

    private record KafkaHeader(org.apache.kafka.common.header.Header header) implements Header {
        @Override
        public String key() {
            return header.key();
        }

        @Override
        public byte[] value() {
            return header.value();
        }
    }

    @Override
    public TopicProducer createProducer(String agentId, StreamingCluster streamingCluster, Map<String, Object> configuration) {
        Map<String, Object> copy = new HashMap<>(configuration);
        applyDefaultConfiguration(agentId, streamingCluster, copy);
        String topicName = (String) copy.remove("topic");

        return new TopicProducer() {

            KafkaProducer<byte[], byte[]> producer;
            Serializer keySerializer = null;
            Serializer valueSerializer = null;
            Serializer headerSerializer = null;

            @Override
            public void start() {
                producer = new KafkaProducer<>(copy);
            }

            @Override
            public void close() {
                if (producer != null) {
                    producer.close();
                }
            }

            @Override
            @SneakyThrows
            public void write(List<Record> records) {
                for (Record r : records) {
                    List<org.apache.kafka.common.header.Header> headers = new ArrayList<>();
                    byte[] key = null;
                    if (r.key() != null) {
                        if (keySerializer == null) {
                            keySerializer = SERIALIZERS.get(r.key().getClass());
                        }
                        key = keySerializer.serialize(topicName, r.key());
                    }
                    byte[] value = null;
                    if (r.value() != null) {
                        if (valueSerializer == null) {
                            valueSerializer = SERIALIZERS.get(r.value().getClass());
                        }
                        value = valueSerializer.serialize(topicName, r.value());
                    }
                    if (r.headers() != null) {
                        for (Header header : r.headers()) {
                            Object headerValue = header.value();
                            byte[] serializedHeader = null;
                            if (headerValue != null) {
                                if (headerSerializer == null) {
                                    headerSerializer = SERIALIZERS.get(headerValue.getClass());
                                }
                                serializedHeader = headerSerializer.serialize(topicName, headerValue);
                            }
                            headers.add(new RecordHeader(header.key(), serializedHeader));
                        }
                    }
                    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topicName, null, null, key, value, headers);
                    log.info("Sending record {}", record);
                    producer.send(record).get();
                }
            }
        };
    }

    private static class KafkaConsumerWrapper implements TopicConsumer {
        private final Map<String, Object> configuration;
        private final String topicName;
        KafkaConsumer consumer;

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
            for (ConsumerRecord<?,?> record : poll) {
                result.add(new KafkaRecord(record));
            }
            log.info("Received {} records from Kafka {}", result.size(), result);
            return result;
        }

        @Override
        public void commit() {
            consumer.commitSync();
        }
    }
}
