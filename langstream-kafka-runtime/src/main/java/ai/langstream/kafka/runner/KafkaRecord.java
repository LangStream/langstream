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

import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.Record;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;

public abstract class KafkaRecord implements Record {

    public static KafkaRecord fromKafkaConsumerRecord(ConsumerRecord<?, ?> record) {
        return new KafkaConsumerRecord(record);
    }

    public static KafkaRecord fromKafkaSourceRecord(SourceRecord record) {
        return new KafkaSourceRecord(record);
    }

    private record KafkaHeader(String key, byte[] value) implements Header {
        public static List<Header> fromKafkaHeaders(
                org.apache.kafka.common.header.Headers headers) {
            List<Header> result = new ArrayList<>();
            for (org.apache.kafka.common.header.Header header : headers) {
                result.add(new KafkaHeader(header.key(), header.value()));
            }
            return result;
        }

        public static List<Header> fromKafkaHeaders(
                org.apache.kafka.connect.header.Headers headers) {
            List<Header> result = new ArrayList<>();
            for (org.apache.kafka.connect.header.Header header : headers) {
                result.add(new KafkaHeader(header.key(), toBytes(header.value(), header.schema())));
            }
            return result;
        }

        // TODO: figure out how to get byte[] out fo the value properly
        private static byte[] toBytes(Object value, Schema schema) {
            if (value instanceof byte[]) {
                return (byte[]) value;
            } else {
                return value.toString().getBytes();
            }
        }

        @Override
        public String valueAsString() {
            return this.value != null ? new String(value, StandardCharsets.UTF_8) : null;
        }
    }

    public interface KafkaSourceOffsetProvider {
        Map<String, ?> sourcePartition();

        Map<String, ?> sourceOffset();
    }

    public interface KafkaConsumerOffsetProvider {
        long offset();

        int estimateRecordSize();

        TopicPartition getTopicPartition();
    }

    @EqualsAndHashCode
    @ToString
    private static class KafkaConsumerRecord extends KafkaRecord
            implements KafkaConsumerOffsetProvider {
        private final ConsumerRecord<?, ?> record;

        public KafkaConsumerRecord(ConsumerRecord<?, ?> record) {
            super(
                    KafkaHeader.fromKafkaHeaders(record.headers()),
                    new TopicPartition(record.topic(), record.partition()));
            this.record = record;
        }

        @Override
        public Object key() {
            return record.key();
        }

        @Override
        public Object value() {
            return record.value();
        }

        @Override
        public int estimateRecordSize() {
            return record.serializedKeySize() + record.serializedValueSize();
        }

        @Override
        public org.apache.kafka.connect.data.Schema keySchema() {
            return null;
        }

        @Override
        public org.apache.kafka.connect.data.Schema valueSchema() {
            return null;
        }

        @Override
        public long offset() {
            return record.offset();
        }

        @Override
        public Long timestamp() {
            return record.timestamp();
        }

        @Override
        public TimestampType timestampType() {
            return record.timestampType();
        }
    }

    @EqualsAndHashCode
    @ToString
    private static class KafkaSourceRecord extends KafkaRecord
            implements KafkaSourceOffsetProvider {
        private final SourceRecord record;

        public KafkaSourceRecord(SourceRecord record) {
            super(
                    KafkaHeader.fromKafkaHeaders(record.headers()),
                    new TopicPartition(
                            record.topic(),
                            record.kafkaPartition() != null ? record.kafkaPartition() : 0));
            this.record = record;
        }

        @Override
        public Object key() {
            return record.key();
        }

        @Override
        public Object value() {
            return record.value();
        }

        @Override
        public Long timestamp() {
            return record.timestamp();
        }

        @Override
        public Schema keySchema() {
            return record.keySchema();
        }

        @Override
        public Schema valueSchema() {
            return record.valueSchema();
        }

        @Override
        public TimestampType timestampType() {
            return TimestampType.NO_TIMESTAMP_TYPE;
        }

        @Override
        public Map<String, ?> sourcePartition() {
            return record.sourcePartition();
        }

        @Override
        public Map<String, ?> sourceOffset() {
            return record.sourceOffset();
        }
    }

    protected final List<Header> headers;
    protected final TopicPartition topicPartition;

    public KafkaRecord(List<Header> headers, TopicPartition topicPartition) {
        this.headers = headers;
        this.topicPartition = topicPartition;
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }

    public abstract org.apache.kafka.connect.data.Schema keySchema();

    public abstract org.apache.kafka.connect.data.Schema valueSchema();

    public String origin() {
        return topicPartition.topic();
    }

    public int partition() {
        return topicPartition.partition();
    }

    public abstract TimestampType timestampType();

    public List<Header> headers() {
        return headers;
    }
}
