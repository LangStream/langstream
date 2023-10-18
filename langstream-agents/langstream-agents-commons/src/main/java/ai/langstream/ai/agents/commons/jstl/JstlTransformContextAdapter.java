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
package ai.langstream.ai.agents.commons.jstl;

import ai.langstream.ai.agents.commons.AvroUtil;
import ai.langstream.ai.agents.commons.MutableRecord;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.Transformer;
import org.apache.commons.collections4.map.LazyMap;

/**
 * A java bean that adapts the underlying {@link MutableRecord} to be ready for jstl expression
 * language binding.
 */
public class JstlTransformContextAdapter {
    private MutableRecord mutableRecord;

    /**
     * A key transformer backing the lazy key map. It transforms top level key fields to either a
     * value or another lazy map for nested evaluation.
     */
    private final Transformer<String, Object> keyTransformer =
            (fieldName) -> {
                Object keyObject = this.mutableRecord.getKeyObject();
                if (keyObject instanceof GenericRecord) {
                    GenericRecord genericRecord = (GenericRecord) keyObject;
                    GenericRecordTransformer transformer =
                            new GenericRecordTransformer(genericRecord);
                    return transformer.transform(fieldName);
                }
                if (keyObject instanceof JsonNode) {
                    JsonNode jsonNode = (JsonNode) keyObject;
                    Schema schema = (Schema) this.mutableRecord.getKeyNativeSchema();
                    JsonNodeTransformer transformer = new JsonNodeTransformer(jsonNode, schema);
                    return transformer.transform(fieldName);
                }
                return null;
            };

    private final Map<String, Object> lazyKey = LazyMap.lazyMap(new HashMap<>(), keyTransformer);

    /**
     * A value transformer backing the lazy value map. It transforms top level value fields to
     * either a value or another lazy map for nested evaluation.
     */
    private final Transformer<String, Object> valueTransformer =
            (fieldName) -> {
                Object valueObject = this.mutableRecord.getValueObject();
                if (valueObject instanceof GenericRecord) {
                    GenericRecord genericRecord = (GenericRecord) valueObject;
                    GenericRecordTransformer transformer =
                            new GenericRecordTransformer(genericRecord);
                    return transformer.transform(fieldName);
                }
                if (valueObject instanceof JsonNode) {
                    JsonNode jsonNode = (JsonNode) valueObject;
                    Schema schema = (Schema) this.mutableRecord.getValueNativeSchema();
                    JsonNodeTransformer transformer = new JsonNodeTransformer(jsonNode, schema);
                    return transformer.transform(fieldName);
                }
                return null;
            };

    private final Map<String, Object> lazyValue =
            LazyMap.lazyMap(new HashMap<>(), valueTransformer);

    /** A header transformer to return message headers the user is allowed to filter on. */
    private final Transformer<String, Object> headerTransformer =
            (fieldName) -> {
                // Allow list message headers in the expression
                switch (fieldName) {
                    case "messageKey":
                        return mutableRecord.getKey();
                    case "topicName":
                        return mutableRecord.getInputTopic();
                    case "destinationTopic":
                        return mutableRecord.getOutputTopic();
                    case "eventTime":
                        return mutableRecord.getEventTime();
                    case "properties":
                        return mutableRecord.getProperties();
                    default:
                        return null;
                }
            };

    private final Map<String, Object> lazyHeader =
            LazyMap.lazyMap(new HashMap<>(), headerTransformer);

    public JstlTransformContextAdapter(MutableRecord mutableRecord) {
        this.mutableRecord = mutableRecord;
    }

    /**
     * @return either a lazily evaluated map to access top-level and nested fields on a generic
     *     object, or the primitive type itself.
     */
    public Object getKey() {
        Object keyObject = this.mutableRecord.getKeyObject();
        if (keyObject == null) {
            return mutableRecord.getKey();
        }
        return keyObject instanceof GenericRecord || keyObject instanceof JsonNode
                ? lazyKey
                : keyObject;
    }

    /**
     * @return either a lazily evaluated map to access top-level and nested fields on a generic
     *     object, or the primitive type itself.
     */
    public Object adaptValue() {
        Object valueObject = this.mutableRecord.getValueObject();
        return valueObject instanceof GenericRecord || valueObject instanceof JsonNode
                ? lazyValue
                : valueObject;
    }

    public Object adaptRecord() {
        return mutableRecord.getRecordObject();
    }

    public Map<String, Object> getHeader() {
        return lazyHeader;
    }

    /** Enables {@link LazyMap} lookup on {@link GenericRecord}. */
    static class GenericRecordTransformer implements Transformer<String, Object> {

        private final GenericRecord genericRecord;

        public GenericRecordTransformer(GenericRecord genericRecord) {
            this.genericRecord = genericRecord;
        }

        @Override
        public Object transform(String key) {
            if (!genericRecord.hasField(key)) {
                return null;
            }
            Object value = genericRecord.get(key);
            if (value instanceof GenericRecord) {
                return LazyMap.lazyMap(
                        new HashMap<>(), new GenericRecordTransformer((GenericRecord) value));
            }
            return adaptValue(genericRecord.getSchema(), key, value);
        }
    }

    static class JsonNodeTransformer implements Transformer<String, Object> {

        public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
        private final JsonNode jsonNode;
        private final org.apache.avro.Schema schema;

        public JsonNodeTransformer(JsonNode jsonNode, org.apache.avro.Schema schema) {
            this.jsonNode = jsonNode;
            this.schema = schema;
        }

        @Override
        public Object transform(String key) {
            if (!jsonNode.has(key)) {
                return null;
            }
            JsonNode node = jsonNode.get(key);

            if (node instanceof ObjectNode) {
                return LazyMap.lazyMap(
                        new HashMap<>(),
                        new JsonNodeTransformer(
                                node, schema != null ? schema.getField(key).schema() : null));
            }
            try {
                Object value = OBJECT_MAPPER.treeToValue(node, Object.class);
                return adaptValue(schema, key, value);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static Object adaptValue(Schema schema, String key, Object value) {
        if (schema == null) {
            return value;
        }
        LogicalType logicalType = AvroUtil.getLogicalType(schema.getField(key).schema());
        if (LogicalTypes.date().equals(logicalType)) {
            return LocalDate.ofEpochDay((int) value);
        } else if (LogicalTypes.timestampMillis().equals(logicalType)) {
            return Instant.ofEpochMilli((long) value);
        } else if (LogicalTypes.timestampMicros().equals(logicalType)) {
            long micros = (long) value;
            return Instant.ofEpochSecond(micros / 1_000_000, (micros % 1_000_000) * 1000);
        } else if (LogicalTypes.timeMillis().equals(logicalType)) {
            return LocalTime.ofNanoOfDay((int) value * 1_000_000L);
        } else if (LogicalTypes.timeMicros().equals(logicalType)) {
            return LocalTime.ofNanoOfDay((long) value * 1_000);
        } else if (LogicalTypes.localTimestampMillis().equals(logicalType)) {
            long millis = (long) value;
            return LocalDateTime.ofEpochSecond(
                    millis / 1_000, (int) ((millis % 1_000) * 1_000_000), ZoneOffset.UTC);
        } else if (LogicalTypes.localTimestampMicros().equals(logicalType)) {
            long micros = (long) value;
            return LocalDateTime.ofEpochSecond(
                    micros / 1_000_000, (int) ((micros % 1_000_000) * 1_000), ZoneOffset.UTC);
        }
        return value;
    }
}
