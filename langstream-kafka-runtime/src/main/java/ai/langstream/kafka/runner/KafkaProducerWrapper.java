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

import static java.util.Map.entry;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.topics.TopicProducer;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
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

@Slf4j
class KafkaProducerWrapper implements TopicProducer {

    static final Map<Class<?>, Serializer<?>> BASE_SERIALIZERS =
            Map.ofEntries(
                    entry(String.class, new StringSerializer()),
                    entry(Boolean.class, new BooleanSerializer()),
                    entry(Short.class, new ShortSerializer()),
                    entry(Integer.class, new IntegerSerializer()),
                    entry(Long.class, new LongSerializer()),
                    entry(Float.class, new FloatSerializer()),
                    entry(Double.class, new DoubleSerializer()),
                    entry(byte[].class, new ByteArraySerializer()),
                    entry(UUID.class, new UUIDSerializer()));

    final Map<Class<?>, Serializer<?>> keySerializers = new ConcurrentHashMap<>(BASE_SERIALIZERS);
    final Map<Class<?>, Serializer<?>> valueSerializers = new ConcurrentHashMap<>(BASE_SERIALIZERS);

    final Map<Class<?>, Serializer<?>> headerSerializers =
            new ConcurrentHashMap<>(BASE_SERIALIZERS);

    private final Map<String, Object> copy;
    private final String topicName;
    private final AtomicInteger totalIn = new AtomicInteger();
    KafkaProducer<Object, Object> producer;
    Serializer keySerializer;
    Class cacheKeyForKeySerializer;
    Serializer valueSerializer;

    Class cacheKeyForValueSerializer;
    Serializer headerSerializer;

    Class cacheKeyForHeaderSerializer;

    boolean forcedKeySerializer;
    boolean forcedValueSerializer;

    public KafkaProducerWrapper(Map<String, Object> copy, String topicName) {
        this.copy = copy;
        this.topicName = topicName;
        keySerializer = null;
        valueSerializer = null;
        headerSerializer = null;
        forcedKeySerializer =
                !Objects.equals(
                        org.apache.kafka.common.serialization.ByteArraySerializer.class.getName(),
                        copy.get(KEY_SERIALIZER_CLASS_CONFIG));
        forcedValueSerializer =
                !Objects.equals(
                        org.apache.kafka.common.serialization.ByteArraySerializer.class.getName(),
                        copy.get(VALUE_SERIALIZER_CLASS_CONFIG));
        if (!forcedKeySerializer) {
            log.debug(
                    "The Producer to {} is configured without a key serializer, we will use reflection to find the right one",
                    topicName);
        } else {
            log.info(
                    "The Producer to {} is configured with a key serializer: {}",
                    topicName,
                    copy.get(KEY_SERIALIZER_CLASS_CONFIG));
        }
        if (!forcedValueSerializer) {
            log.debug(
                    "The Producer to {} is configured without a value serializer, we will use reflection to find the right one",
                    topicName);
        } else {
            log.info(
                    "The Producer to {} is configured with a value serializer: {}",
                    topicName,
                    copy.get(VALUE_SERIALIZER_CLASS_CONFIG));
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
    public long getTotalIn() {
        return totalIn.get();
    }

    @Override
    public Map<String, Object> getInfo() {
        Map<String, Object> result = new HashMap<>();
        if (producer != null) {
            result.put("kafkaProducerMetrics", KafkaMetricsUtils.metricsToMap(producer.metrics()));
        }
        return result;
    }

    @Override
    public synchronized CompletableFuture<?> write(Record r) {
        CompletableFuture<?> handle = new CompletableFuture<>();
        try {
            List<org.apache.kafka.common.header.Header> headers = new ArrayList<>();
            Object key = null;
            if (r.key() != null) {
                if (forcedKeySerializer) {
                    key = r.key();
                } else {
                    Class<?> keyClass = r.key().getClass();
                    if (keySerializer == null
                            || !(Objects.equals(keyClass, cacheKeyForKeySerializer))) {
                        keySerializer = getSerializer(keyClass, keySerializers, true);
                        cacheKeyForKeySerializer = keyClass;
                    }
                    key = keySerializer.serialize(topicName, r.key());
                }
            }
            Object value = null;
            if (r.value() != null) {
                if (forcedValueSerializer) {
                    value = r.value();
                } else {
                    Class<?> valueClass = r.value().getClass();
                    if (valueSerializer == null
                            || !(Objects.equals(valueClass, cacheKeyForValueSerializer))) {
                        valueSerializer = getSerializer(valueClass, valueSerializers, false);
                        cacheKeyForValueSerializer = valueClass;
                    }
                    value = valueSerializer.serialize(topicName, r.value());
                }
            }
            if (r.headers() != null) {
                for (Header header : r.headers()) {
                    Object headerValue = header.value();
                    byte[] serializedHeader = null;

                    if (headerValue != null) {
                        Class<?> headerClass = headerValue.getClass();
                        if (headerSerializer == null
                                || !(Objects.equals(headerClass, cacheKeyForHeaderSerializer))) {
                            headerSerializer = getSerializer(headerClass, headerSerializers, null);
                            cacheKeyForHeaderSerializer = headerClass;
                        }
                        serializedHeader = headerSerializer.serialize(topicName, headerValue);
                    }
                    headers.add(new RecordHeader(header.key(), serializedHeader));
                }
            }
            ProducerRecord<Object, Object> record =
                    new ProducerRecord<>(topicName, null, null, key, value, headers);

            if (log.isDebugEnabled()) {
                log.debug("Sending record {}", record);
            }

            producer.send(
                    record,
                    (metadata, exception) -> {
                        if (exception != null) {
                            handle.completeExceptionally(exception);
                        } else {
                            totalIn.addAndGet(1);
                            handle.complete(null);
                        }
                    });
        } catch (Exception e) {
            handle.completeExceptionally(e);
        }
        return handle;
    }

    private Serializer<?> getSerializer(
            Class<?> r, Map<Class<?>, Serializer<?>> serializerMap, Boolean isKey) {
        return serializerMap.computeIfAbsent(
                r,
                k -> {
                    if (GenericRecord.class.isAssignableFrom(k)
                            && isKey != null) { // no AVRO in headers
                        KafkaAvroSerializer kafkaAvroSerializer = new KafkaAvroSerializer();
                        kafkaAvroSerializer.configure(copy, isKey);
                        serializerMap.put(r, kafkaAvroSerializer);
                        return kafkaAvroSerializer;
                    }
                    if (Map.class.isAssignableFrom(k)) {
                        return new ObjectToJsonSerializer();
                    }
                    if (Collection.class.isAssignableFrom(k)) {
                        return new ObjectToJsonSerializer();
                    }
                    throw new IllegalArgumentException("Cannot find a serializer for " + r);
                });
    }

    @Override
    public String toString() {
        return "KafkaProducerWrapper{" + "topicName='" + topicName + '\'' + '}';
    }

    private static class ObjectToJsonSerializer implements Serializer<Object> {
        private static final ObjectMapper MAPPER = new ObjectMapper();

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {}

        @Override
        @SneakyThrows
        public byte[] serialize(String topic, Object data) {
            return MAPPER.writeValueAsBytes(data);
        }
    }
}
