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
package ai.langstream.ai.agents.commons;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

@Slf4j
@Data
public class TransformContext {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private TransformSchemaType keySchemaType;
    private Object keyNativeSchema;
    private Object keyObject;
    private TransformSchemaType valueSchemaType;
    private Object valueNativeSchema;
    private Object valueObject;
    private String key;
    private Map<String, String> properties;
    private String inputTopic;
    private String outputTopic;
    private Long eventTime;
    private boolean dropCurrentRecord;
    Map<String, Object> customContext = new HashMap<>();
    // only for fn:filter
    private Object recordObject;

    public TransformContext copy() {
        TransformContext copy = new TransformContext();

        copy.keyObject = safeClone(keyObject);
        copy.valueObject = safeClone(valueObject);

        copy.properties =
                properties != null
                        ? new HashMap<>(properties)
                        : null; // no need for deep clone here
        copy.customContext = new HashMap<>(customContext); // no need for deep clone here

        // immutable data structures, they are safe to copy by reference
        copy.key = key;
        copy.keySchemaType = keySchemaType;
        copy.valueSchemaType = valueSchemaType;
        copy.keyNativeSchema = keyNativeSchema;
        copy.valueNativeSchema = valueNativeSchema;
        copy.inputTopic = inputTopic;
        copy.outputTopic = outputTopic;
        copy.eventTime = eventTime;
        copy.dropCurrentRecord = dropCurrentRecord;

        if (recordObject != null) {
            throw new UnsupportedOperationException(
                    "Cannot copy a TransformContext with a recordObject");
        }
        return copy;
    }

    public void convertMapToStringOrBytes() throws JsonProcessingException {
        if (valueObject instanceof Map) {
            if (valueSchemaType == TransformSchemaType.STRING) {
                valueObject = OBJECT_MAPPER.writeValueAsString(valueObject);
            } else if (valueSchemaType == TransformSchemaType.BYTES) {
                valueObject = OBJECT_MAPPER.writeValueAsBytes(valueObject);
            }
        }
        if (keyObject instanceof Map) {
            if (keySchemaType == TransformSchemaType.STRING) {
                keyObject = OBJECT_MAPPER.writeValueAsString(keyObject);
            } else if (keySchemaType == TransformSchemaType.BYTES) {
                keyObject = OBJECT_MAPPER.writeValueAsBytes(keyObject);
            }
        }
    }

    public void convertAvroToBytes() throws IOException {
        if (keySchemaType == TransformSchemaType.AVRO) {
            keyObject = serializeGenericRecord((GenericRecord) keyObject);
        }
        if (valueSchemaType == TransformSchemaType.AVRO) {
            valueObject = serializeGenericRecord((GenericRecord) valueObject);
        }
    }

    public void setProperty(String key, String value) {
        if (this.properties == null) {
            this.properties = new HashMap<>();
        }
        this.properties.put(key, value);
    }

    public static byte[] serializeGenericRecord(GenericRecord record) throws IOException {
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());
        // enable Decimal conversion, otherwise attempting to serialize java.math.BigDecimal will
        // throw
        // ClassCastException
        writer.getData().addLogicalTypeConversion(new Conversions.DecimalConversion());
        ByteArrayOutputStream oo = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(oo, null);
        writer.write(record, encoder);
        return oo.toByteArray();
    }

    public void dropValueFields(Collection<String> fields, Map<Schema, Schema> schemaCache) {
        if (valueObject instanceof Map) {
            fields.forEach(((Map<?, ?>) valueObject)::remove);
        } else if (valueSchemaType == TransformSchemaType.AVRO) {
            dropAvroValueFields(fields, schemaCache);
        } else if (valueSchemaType == TransformSchemaType.JSON) {
            dropJsonValueFields(fields, schemaCache);
        }
    }

    private void dropAvroValueFields(Collection<String> fields, Map<Schema, Schema> schemaCache) {
        GenericRecord avroRecord = (GenericRecord) valueObject;
        GenericRecord newRecord = AvroUtil.dropAvroRecordFields(avroRecord, fields, schemaCache);
        valueNativeSchema = newRecord.getSchema();
        valueObject = newRecord;
    }

    private void dropJsonValueFields(Collection<String> fields, Map<Schema, Schema> schemaCache) {
        if (valueNativeSchema != null) {
            valueNativeSchema =
                    AvroUtil.dropAvroSchemaFields((Schema) valueNativeSchema, fields, schemaCache);
        }
        valueObject = ((ObjectNode) valueObject).remove(fields);
    }

    public void dropKeyFields(Collection<String> fields, Map<Schema, Schema> schemaCache) {
        if (keyObject instanceof Map) {
            fields.forEach(((Map<?, ?>) keyObject)::remove);
        } else if (keySchemaType == TransformSchemaType.AVRO) {
            dropAvroKeyFields(fields, schemaCache);
        } else if (keySchemaType == TransformSchemaType.JSON) {
            dropJsonKeyFields(fields, schemaCache);
        }
    }

    private void dropAvroKeyFields(Collection<String> fields, Map<Schema, Schema> schemaCache) {
        GenericRecord avroRecord = (GenericRecord) keyObject;
        GenericRecord newRecord = AvroUtil.dropAvroRecordFields(avroRecord, fields, schemaCache);
        keyNativeSchema = newRecord.getSchema();
        keyObject = newRecord;
    }

    private void dropJsonKeyFields(Collection<String> fields, Map<Schema, Schema> schemaCache) {
        if (keyNativeSchema != null) {
            keyNativeSchema =
                    AvroUtil.dropAvroSchemaFields((Schema) keyNativeSchema, fields, schemaCache);
        }
        keyObject = ((ObjectNode) keyObject).remove(fields);
    }

    public void addOrReplaceValueFields(
            Map<Schema.Field, Object> newFields, Map<Schema, Schema> schemaCache) {
        if (valueSchemaType == TransformSchemaType.AVRO) {
            addOrReplaceAvroValueFields(newFields, schemaCache);
        } else if (valueSchemaType == TransformSchemaType.JSON) {
            addOrReplaceJsonValueFields(newFields, schemaCache);
        }
    }

    private void addOrReplaceAvroValueFields(
            Map<Schema.Field, Object> newFields, Map<Schema, Schema> schemaCache) {
        GenericRecord avroRecord = (GenericRecord) valueObject;
        GenericRecord newRecord =
                AvroUtil.addOrReplaceAvroRecordFields(avroRecord, newFields, schemaCache);
        valueNativeSchema = newRecord.getSchema();
        valueObject = newRecord;
    }

    private void addOrReplaceJsonValueFields(
            Map<Schema.Field, Object> newFields, Map<Schema, Schema> schemaCache) {
        if (valueNativeSchema != null) {
            valueNativeSchema =
                    AvroUtil.addOrReplaceAvroSchemaFields(
                            (Schema) valueNativeSchema, newFields.keySet(), schemaCache);
        }
        ObjectNode json = (ObjectNode) valueObject;
        newFields.forEach(
                (field, value) -> json.set(field.name(), OBJECT_MAPPER.valueToTree(value)));
        valueObject = json;
    }

    public void addOrReplaceKeyFields(
            Map<Schema.Field, Object> newFields, Map<Schema, Schema> schemaCache) {
        if (keySchemaType == TransformSchemaType.AVRO) {
            addOrReplaceAvroKeyFields(newFields, schemaCache);
        } else if (keySchemaType == TransformSchemaType.JSON) {
            addOrReplaceJsonKeyFields(newFields, schemaCache);
        }
    }

    private void addOrReplaceAvroKeyFields(
            Map<Schema.Field, Object> newFields, Map<Schema, Schema> schemaCache) {
        GenericRecord avroRecord = (GenericRecord) keyObject;
        GenericRecord newRecord =
                AvroUtil.addOrReplaceAvroRecordFields(avroRecord, newFields, schemaCache);
        keyNativeSchema = newRecord.getSchema();
        keyObject = newRecord;
    }

    private void addOrReplaceJsonKeyFields(
            Map<Schema.Field, Object> newFields, Map<Schema, Schema> schemaCache) {
        if (keyNativeSchema != null) {
            keyNativeSchema =
                    AvroUtil.addOrReplaceAvroSchemaFields(
                            (Schema) keyNativeSchema, newFields.keySet(), schemaCache);
        }
        ObjectNode json = (ObjectNode) keyObject;
        newFields.forEach(
                (field, value) -> json.set(field.name(), OBJECT_MAPPER.valueToTree(value)));
        keyObject = json;
    }

    public JsonRecord toJsonRecord() {
        JsonRecord jsonRecord = new JsonRecord();
        if (keySchemaType != null) {
            jsonRecord.setKey(toJsonSerializable(keySchemaType, keyObject));
        } else {
            jsonRecord.setKey(key);
        }
        jsonRecord.setValue(toJsonSerializable(valueSchemaType, valueObject));
        jsonRecord.setDestinationTopic(outputTopic);

        jsonRecord.setProperties(properties);
        jsonRecord.setEventTime(eventTime);
        jsonRecord.setTopicName(inputTopic);
        return jsonRecord;
    }

    private static Object toJsonSerializable(TransformSchemaType schemaType, Object val) {
        if (schemaType == null || schemaType.isPrimitive()) {
            return val;
        }
        switch (schemaType) {
            case AVRO:
                // TODO: do better than the double conversion AVRO -> JsonNode -> Map
                return OBJECT_MAPPER.convertValue(
                        JsonConverter.toJson((GenericRecord) val),
                        new TypeReference<Map<String, Object>>() {});
            case JSON:
                return OBJECT_MAPPER.convertValue(val, new TypeReference<Map<String, Object>>() {});
            default:
                throw new UnsupportedOperationException("Unsupported schemaType " + schemaType);
        }
    }

    @SneakyThrows
    public static String toJson(Object object) {
        return OBJECT_MAPPER.writeValueAsString(object);
    }

    public void setResultField(
            Object content,
            String fieldName,
            Schema fieldSchema,
            Map<Schema, Schema> avroKeySchemaCache,
            Map<Schema, Schema> avroValueSchemaCache) {
        if (fieldName == null || fieldName.equals("value")) {
            valueSchemaType = TransformSchemaType.STRING;
            valueObject = content;
        } else if (fieldName.equals("key")) {
            keySchemaType = TransformSchemaType.STRING;
            keyObject = content;
        } else if (fieldName.equals("destinationTopic")) {
            outputTopic = content.toString();
        } else if (fieldName.equals("messageKey")) {
            key = content.toString();
        } else if (fieldName.startsWith("properties.")) {
            String propertyKey = fieldName.substring("properties.".length());
            setProperty(propertyKey, content.toString());
        } else if (fieldName.startsWith("value.")) {
            String valueFieldName = fieldName.substring("value.".length());
            if (valueObject instanceof Map) {
                ((Map<String, Object>) valueObject).put(valueFieldName, content);
            } else {
                Schema.Field fieldSchemaField =
                        new Schema.Field(valueFieldName, fieldSchema, null, null);
                addOrReplaceValueFields(Map.of(fieldSchemaField, content), avroValueSchemaCache);
            }
        } else if (fieldName.startsWith("key.")) {
            String keyFieldName = fieldName.substring("key.".length());
            if (keyObject instanceof Map) {
                ((Map<String, Object>) keyObject).put(keyFieldName, content);
            } else {
                Schema.Field fieldSchemaField =
                        new Schema.Field(keyFieldName, fieldSchema, null, null);
                addOrReplaceKeyFields(Map.of(fieldSchemaField, content), avroKeySchemaCache);
            }
        }
    }

    public static Object safeClone(Object object) {
        if (object == null) {
            return null;
        }
        if (object.getClass().isPrimitive()
                || object instanceof String
                || object instanceof Number
                || object instanceof Boolean) {
            return object;
        }
        if (object instanceof Map map) {
            HashMap<Object, Object> res = new HashMap<>();
            map.forEach((k, v) -> res.put(safeClone(k), safeClone(v)));
            return res;
        }
        if (object instanceof List list) {
            List<Object> res = new ArrayList<>();
            list.forEach(v -> res.add(safeClone(v)));
            return res;
        }
        if (object instanceof Set set) {
            Set<Object> res = new HashSet<>();
            set.forEach(v -> res.add(safeClone(v)));
            return res;
        }
        if (object instanceof GenericRecord genericRecord) {
            return GenericData.get().deepCopy(genericRecord.getSchema(), genericRecord);
        }
        if (object instanceof JsonNode jsonNode) {
            return jsonNode.deepCopy();
        }
        throw new UnsupportedOperationException("Cannot copy a value of " + object.getClass());
    }
}
