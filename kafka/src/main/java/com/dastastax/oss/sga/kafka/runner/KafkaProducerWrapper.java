package com.dastastax.oss.sga.kafka.runner;

import com.datastax.oss.sga.api.runner.code.Header;
import com.datastax.oss.sga.api.runner.code.Record;
import com.datastax.oss.sga.api.runner.topics.TopicProducer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
class KafkaProducerWrapper implements TopicProducer {
    static final Map<Class<?>, Serializer<?>> SERIALIZERS = Map.of(
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

    private final Map<String, Object> copy;
    private final String topicName;
    KafkaProducer<byte[], byte[]> producer;
    Serializer keySerializer;
    Serializer valueSerializer;
    Serializer headerSerializer;

    public KafkaProducerWrapper(Map<String, Object> copy, String topicName) {
        this.copy = copy;
        this.topicName = topicName;
        keySerializer = null;
        valueSerializer = null;
        headerSerializer = null;
    }

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
}
