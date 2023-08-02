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
package com.dastastax.oss.sga.kafka.runner;

import com.datastax.oss.sga.api.runner.code.Header;
import com.datastax.oss.sga.api.runner.code.Record;
import com.datastax.oss.sga.api.runner.topics.TopicProducer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

@Slf4j
class KafkaProducerWrapper implements TopicProducer {

    final Map<Class<?>, Serializer<?>> BASE_SERIALIZERS = Map.of(
            String.class, new StringSerializer(),
            Boolean.class, new BooleanSerializer(),
            Short.class, new ShortSerializer(),
            Integer.class, new IntegerSerializer(),
            Long.class, new LongSerializer(),
            Float.class, new FloatSerializer(),
            Double.class, new DoubleSerializer(),
            byte[].class, new ByteArraySerializer(),
            UUID.class, new UUIDSerializer());

    final Map<Class<?>, Serializer<?>> keySerializers = new ConcurrentHashMap<>(BASE_SERIALIZERS);
    final Map<Class<?>, Serializer<?>> valueSerializers = new ConcurrentHashMap<>(BASE_SERIALIZERS);

    final Map<Class<?>, Serializer<?>> headerSerializers = new ConcurrentHashMap<>(BASE_SERIALIZERS);

    private final Map<String, Object> copy;
    private final String topicName;
    KafkaProducer<Object, Object> producer;
    Serializer keySerializer;
    Serializer valueSerializer;
    Serializer headerSerializer;

    boolean forcedKeySerializer;
    boolean forcedValueSerializer;

    public KafkaProducerWrapper(Map<String, Object> copy, String topicName) {
        this.copy = copy;
        this.topicName = topicName;
        keySerializer = null;
        valueSerializer = null;
        headerSerializer = null;
        forcedKeySerializer = !Objects.equals(org.apache.kafka.common.serialization.ByteArraySerializer.class.getName(),
                copy.get(KEY_SERIALIZER_CLASS_CONFIG));
        forcedValueSerializer = !Objects.equals(org.apache.kafka.common.serialization.ByteArraySerializer.class.getName(),
                copy.get(VALUE_SERIALIZER_CLASS_CONFIG));
        if (!forcedKeySerializer) {
            log.info("The Producer is configured without a key serializer, we will use reflection to find the right one");
        } else {
            log.info("The Producer is configured with a key serializer: {}", copy.get(KEY_SERIALIZER_CLASS_CONFIG));
        }
        if (!forcedValueSerializer) {
            log.info("The Producer is configured without a value serializer, we will use reflection to find the right one");
        } else {
            log.info("The Producer is configured with a value serializer: {}", copy.get(VALUE_SERIALIZER_CLASS_CONFIG));
        }
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
    public Object getNativeProducer() {
        return producer;
    }

    @Override
    public Map<String, Object> getInfo() {
        Map<String, Object> result = new HashMap<>();
        if (producer != null) {
            Map<MetricName, ? extends Metric> metrics = producer.metrics();
            if (topicName != null) {
                result.put("topicName", topicName);
            }
            result.put("kafkaProducerMetrics", metrics
                    .values()
                    .stream()
                    .collect(Collectors.toMap(m->m.metricName().name(), Metric::metricValue)));
        }
        return result;
    }

    @Override
    @SneakyThrows
    public void write(List<Record> records) {
        for (Record r : records) {
            List<org.apache.kafka.common.header.Header> headers = new ArrayList<>();
            Object key = null;
            if (r.key() != null) {
                if (forcedKeySerializer) {
                    key = r.key();
                } else {
                    if (keySerializer == null) {
                        keySerializer = getSerializer(r.key().getClass(), keySerializers, true);
                    }
                    key = keySerializer.serialize(topicName, r.key());
                }
            }
            Object value = null;
            if (r.value() != null) {
                if (forcedValueSerializer) {
                    value = r.value();
                } else {
                    if (valueSerializer == null) {
                        valueSerializer = getSerializer(r.value().getClass(), valueSerializers, false);
                    }
                    value = valueSerializer.serialize(topicName, r.value());
                }
            }
            if (r.headers() != null) {
                for (Header header : r.headers()) {
                    Object headerValue = header.value();
                    byte[] serializedHeader = null;
                    if (headerValue != null) {
                        if (headerSerializer == null) {
                            headerSerializer = getSerializer(headerValue.getClass(), headerSerializers, null);
                        }
                        serializedHeader = headerSerializer.serialize(topicName, headerValue);
                    }
                    headers.add(new RecordHeader(header.key(), serializedHeader));
                }
            }
            ProducerRecord<Object, Object> record = new ProducerRecord<>(topicName, null, null, key, value, headers);
            log.info("Sending record {}", record);
            producer.send(record).get();
        }
    }

    private Serializer<?> getSerializer(Class<?> r, Map<Class<?>, Serializer<?>> serializerMap, Boolean isKey) {
        Serializer<?> result = serializerMap.get(r);
        if (result == null) {
            if (GenericRecord.class.isAssignableFrom(r)
                    && isKey != null) { // no AVRO in headers
                KafkaAvroSerializer kafkaAvroSerializer = new KafkaAvroSerializer();
                kafkaAvroSerializer.configure(copy, isKey);
                serializerMap.put(r, kafkaAvroSerializer);
                return kafkaAvroSerializer;
            }
            throw new IllegalArgumentException("Cannot find a serializer for " + r);
        }
        return result;
    }
}
