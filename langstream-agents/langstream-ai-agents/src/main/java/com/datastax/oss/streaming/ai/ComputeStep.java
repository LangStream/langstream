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
package com.datastax.oss.streaming.ai;

import ai.langstream.ai.agents.commons.AvroUtil;
import ai.langstream.ai.agents.commons.MutableRecord;
import ai.langstream.ai.agents.commons.TransformSchemaType;
import ai.langstream.ai.agents.commons.jstl.JstlFunctions;
import ai.langstream.ai.agents.commons.jstl.JstlTypeConverter;
import com.datastax.oss.streaming.ai.model.ComputeField;
import com.datastax.oss.streaming.ai.model.ComputeFieldType;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

/** Computes a field dynamically based on JSTL expressions and adds it to the key or the value . */
@Builder
@Slf4j
public class ComputeStep implements TransformStep {
    public static final long MILLIS_PER_DAY = TimeUnit.DAYS.toMillis(1);
    @Builder.Default private final List<ComputeField> fields = new ArrayList<>();
    private final Map<org.apache.avro.Schema, org.apache.avro.Schema> keySchemaCache =
            new ConcurrentHashMap<>();
    private final Map<org.apache.avro.Schema, org.apache.avro.Schema> valueSchemaCache =
            new ConcurrentHashMap<>();
    private final Map<ComputeFieldType, org.apache.avro.Schema> fieldTypeToAvroSchemaCache =
            new ConcurrentHashMap<>();

    @Override
    public void process(MutableRecord mutableRecord) {
        try (JstlFunctions.FilterContextHandle handle =
                JstlFunctions.FilterContextHandle.start(mutableRecord)) {
            computePrimitiveField(
                    fields.stream()
                            .filter(f -> "primitive".equals(f.getScope()))
                            .collect(Collectors.toList()),
                    mutableRecord);
            computeKeyFields(
                    fields.stream()
                            .filter(f -> "key".equals(f.getScope()))
                            .collect(Collectors.toList()),
                    mutableRecord);
            computeValueFields(
                    fields.stream()
                            .filter(f -> "value".equals(f.getScope()))
                            .collect(Collectors.toList()),
                    mutableRecord);
            computeHeaderFields(
                    fields.stream()
                            .filter(f -> "header".equals(f.getScope()))
                            .collect(Collectors.toList()),
                    mutableRecord);
            computeHeaderPropertiesFields(
                    fields.stream()
                            .filter(f -> "header.properties".equals(f.getScope()))
                            .collect(Collectors.toList()),
                    mutableRecord);
        } catch (RuntimeException error) {
            log.error("Error while computing fields on record {}", mutableRecord, error);
            throw error;
        }
    }

    public void computeValueFields(List<ComputeField> fields, MutableRecord context) {
        TransformSchemaType schemaType = context.getValueSchemaType();
        if (context.getValueObject() instanceof Map) {
            getEvaluatedFields(fields, context)
                    .forEach(
                            (field, value) ->
                                    ((Map) context.getValueObject()).put(field.name(), value));
        } else if (schemaType == TransformSchemaType.AVRO
                || schemaType == TransformSchemaType.JSON) {
            Map<Schema.Field, Object> evaluatedFields = getEvaluatedFields(fields, context);
            context.addOrReplaceValueFields(evaluatedFields, valueSchemaCache);
        }
    }

    public void computePrimitiveField(List<ComputeField> fields, MutableRecord context) {
        fields.stream()
                .filter(f -> "key".equals(f.getName()))
                .findFirst()
                .ifPresent(
                        field -> {
                            if (context.getKeySchemaType() != null
                                    && context.getKeySchemaType().isPrimitive()) {
                                Object newKey = field.getEvaluator().evaluate(context);
                                TransformSchemaType newSchema;
                                if (field.getType() != null) {
                                    newSchema = getPrimitiveSchema(field.getType());
                                } else {
                                    newSchema = getPrimitiveSchema(newKey);
                                }
                                context.setKeyObject(newKey);
                                context.setKeySchemaType(newSchema);
                            }
                        });

        fields.stream()
                .filter(f -> "value".equals(f.getName()))
                .findFirst()
                .ifPresent(
                        field -> {
                            if (context.getValueSchemaType().isPrimitive()) {
                                Object newValue = field.getEvaluator().evaluate(context);
                                TransformSchemaType newSchema;
                                if (field.getType() != null) {
                                    newSchema = getPrimitiveSchema(field.getType());
                                } else {
                                    newSchema = getPrimitiveSchema(newValue);
                                }
                                context.setValueObject(newValue);
                                context.setValueSchemaType(newSchema);
                            }
                        });
    }

    public void computeKeyFields(List<ComputeField> fields, MutableRecord context) {
        Object keyObject = context.getKeyObject();
        if (keyObject != null) {
            if (keyObject instanceof Map) {
                getEvaluatedFields(fields, context)
                        .forEach((field, value) -> ((Map) keyObject).put(field.name(), value));
            } else {
                TransformSchemaType schemaType = context.getKeySchemaType();
                if (schemaType == TransformSchemaType.AVRO
                        || schemaType == TransformSchemaType.JSON) {
                    Map<Schema.Field, Object> evaluatedFields = getEvaluatedFields(fields, context);
                    context.addOrReplaceKeyFields(evaluatedFields, keySchemaCache);
                }
            }
        }
    }

    public void computeHeaderFields(List<ComputeField> fields, MutableRecord context) {
        fields.forEach(
                field -> {
                    switch (field.getName()) {
                        case "destinationTopic":
                            String topic = validateAndGetString(field, context);
                            context.setOutputTopic(topic);
                            break;
                        case "messageKey":
                            String key = validateAndGetString(field, context);
                            context.setKey(key);
                            break;
                        default:
                            throw new IllegalArgumentException(
                                    "Invalid compute field name: " + field.getName());
                    }
                });
    }

    public void computeHeaderPropertiesFields(List<ComputeField> fields, MutableRecord context) {
        fields.forEach(
                field ->
                        context.setProperty(field.getName(), validateAndGetString(field, context)));
    }

    private String validateAndGetString(ComputeField field, MutableRecord context) {
        Object value = field.getEvaluator().evaluate(context);
        if (value instanceof String) {
            return (String) value;
        }

        throw new IllegalArgumentException(
                String.format(
                        "Invalid compute field type. " + "Name: %s, Type: %s, Expected Type: %s",
                        field.getName(),
                        value == null ? "null" : value.getClass().getSimpleName(),
                        "String"));
    }

    private Map<Schema.Field, Object> getEvaluatedFields(
            List<ComputeField> fields, MutableRecord context) {
        Map<Schema.Field, Object> evaluatedFields =
                new LinkedHashMap<>(); // preserves the insertion order of keys
        for (ComputeField field : fields) {
            Object value = field.getEvaluator().evaluate(context);
            ComputeFieldType type = field.getType() == null ? getFieldType(value) : field.getType();
            Schema.Field avroField = createAvroField(field, type, value);
            evaluatedFields.put(avroField, getAvroValue(avroField.schema(), value));
        }
        return evaluatedFields;
    }

    private Object getAvroValue(Schema schema, Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof byte[]) {
            return ByteBuffer.wrap((byte[]) value);
        }

        if (value instanceof Byte || value instanceof Short) {
            return ((Number) value).intValue();
        }

        LogicalType logicalType = AvroUtil.getLogicalType(schema);
        if (logicalType == null) {
            return value;
        }

        // Avro logical type conversion: https://avro.apache.org/docs/1.8.2/spec.html#Logical+Types
        switch (logicalType.getName()) {
            case "date":
                return getAvroDate(value, schema.getLogicalType());
            case "time-millis":
                return getAvroTimeMillis(value, schema.getLogicalType());
            case "timestamp-millis":
                return getAvroTimestampMillis(value, schema.getLogicalType());
            case "decimal":
                return getAvroDecimal(value, schema.getLogicalType());
        }

        throw new IllegalArgumentException(
                String.format(
                        "Invalid logical type %s for value %s", schema.getLogicalType(), value));
    }

    private Long getAvroTimestampMillis(Object value, LogicalType logicalType) {
        validateLogicalType(
                value, logicalType, Instant.class, Timestamp.class, LocalDateTime.class);
        return JstlTypeConverter.INSTANCE.coerceToType(value, Long.class);
    }

    private Integer getAvroDate(Object value, LogicalType logicalType) {
        validateLogicalType(value, logicalType, Date.class, LocalDate.class);
        return (value instanceof LocalDate)
                ? Math.toIntExact(((LocalDate) value).toEpochDay())
                : Math.toIntExact((((Date) value).getTime() / MILLIS_PER_DAY));
    }

    private Integer getAvroTimeMillis(Object value, LogicalType logicalType) {
        validateLogicalType(value, logicalType, Time.class, LocalTime.class);
        return JstlTypeConverter.INSTANCE.coerceToType(value, Integer.class);
    }

    private BigDecimal getAvroDecimal(Object value, LogicalType logicalType) {
        validateLogicalType(value, logicalType, BigDecimal.class);
        return JstlTypeConverter.INSTANCE.coerceToType(value, BigDecimal.class);
    }

    void validateLogicalType(Object value, LogicalType logicalType, Class<?>... expectedClasses) {
        for (Class<?> clazz : expectedClasses) {
            if ((value.getClass().equals(clazz))) {
                return;
            }
        }
        throw new IllegalArgumentException(
                String.format(
                        "Invalid java type %s for logical type %s", value.getClass(), logicalType));
    }

    private Schema.Field createAvroField(ComputeField field, ComputeFieldType type, Object value) {
        Schema avroSchema = getAvroSchema(type, value);
        Object defaultValue = null;
        if (field.isOptional()) {
            avroSchema = SchemaBuilder.unionOf().nullType().and().type(avroSchema).endUnion();
            defaultValue = Schema.Field.NULL_DEFAULT_VALUE;
        }
        return new Schema.Field(field.getName(), avroSchema, null, defaultValue);
    }

    private Schema getAvroSchema(ComputeFieldType type, Object value) {
        Schema.Type schemaType;
        switch (type) {
            case STRING:
                schemaType = Schema.Type.STRING;
                break;
            case INT8:
            case INT16:
            case INT32:
            case DATE:
            case LOCAL_DATE:
            case TIME:
            case LOCAL_TIME:
                schemaType = Schema.Type.INT;
                break;
            case INT64:
            case DATETIME:
            case TIMESTAMP:
            case INSTANT:
            case LOCAL_DATE_TIME:
                schemaType = Schema.Type.LONG;
                break;
            case FLOAT:
                schemaType = Schema.Type.FLOAT;
                break;
            case DOUBLE:
                schemaType = Schema.Type.DOUBLE;
                break;
            case BOOLEAN:
                schemaType = Schema.Type.BOOLEAN;
                break;
            case BYTES:
                schemaType = Schema.Type.BYTES;
                break;
            case MAP:
                schemaType = Schema.Type.MAP;
                break;
            case ARRAY:
                schemaType = Schema.Type.ARRAY;
                break;
            case DECIMAL:
                // disable caching for decimal schema because the schema is different for each
                // precision and
                // scale combo and will result in an arbitrary numbers of schemas
                // See: https://avro.apache.org/docs/1.10.2/spec.html#Decimal
                BigDecimal decimal = (BigDecimal) value;
                return LogicalTypes.decimal(decimal.precision(), decimal.scale())
                        .addToSchema(Schema.create(Schema.Type.BYTES));
            default:
                throw new UnsupportedOperationException("Unsupported compute field type: " + type);
        }

        return fieldTypeToAvroSchemaCache.computeIfAbsent(
                type,
                key -> {
                    if (schemaType == Schema.Type.ARRAY) {
                        // we don't know the element type of the array, so we can't create a schema
                        return Schema.createArray(
                                Schema.createMap(Schema.create(Schema.Type.STRING)));
                    }
                    if (schemaType == Schema.Type.MAP) {
                        // we don't know the element type of the array, so we can't create a schema
                        return Schema.createMap(Schema.create(Schema.Type.STRING));
                    }

                    // Handle logical types:
                    // https://avro.apache.org/docs/1.10.2/spec.html#Logical+Types
                    Schema schema = Schema.create(schemaType);
                    switch (key) {
                        case DATE:
                        case LOCAL_DATE:
                            return LogicalTypes.date().addToSchema(schema);
                        case TIME:
                        case LOCAL_TIME:
                            return LogicalTypes.timeMillis().addToSchema(schema);
                        case DATETIME:
                        case INSTANT:
                        case TIMESTAMP:
                        case LOCAL_DATE_TIME:
                            return LogicalTypes.timestampMillis().addToSchema(schema);
                        default:
                            return schema;
                    }
                });
    }

    private TransformSchemaType getPrimitiveSchema(ComputeFieldType type) {
        switch (type) {
            case STRING:
                return TransformSchemaType.STRING;
            case INT8:
                return TransformSchemaType.INT8;
            case INT16:
                return TransformSchemaType.INT16;
            case INT32:
                return TransformSchemaType.INT32;
            case INT64:
                return TransformSchemaType.INT64;
            case FLOAT:
                return TransformSchemaType.FLOAT;
            case DOUBLE:
                return TransformSchemaType.DOUBLE;
            case BOOLEAN:
                return TransformSchemaType.BOOLEAN;
            case DATE:
                return TransformSchemaType.DATE;
            case LOCAL_DATE:
                return TransformSchemaType.LOCAL_DATE;
            case TIME:
                return TransformSchemaType.TIME;
            case LOCAL_TIME:
                return TransformSchemaType.LOCAL_TIME;
            case LOCAL_DATE_TIME:
                return TransformSchemaType.LOCAL_DATE_TIME;
            case DATETIME:
            case INSTANT:
                return TransformSchemaType.INSTANT;
            case TIMESTAMP:
                return TransformSchemaType.TIMESTAMP;
            case BYTES:
                return TransformSchemaType.BYTES;
            default:
                throw new UnsupportedOperationException("Unsupported compute field type: " + type);
        }
    }

    private TransformSchemaType getPrimitiveSchema(Object value) {
        if (value == null) {
            throw new UnsupportedOperationException("Cannot get schema from null value");
        }
        if (value.getClass().equals(String.class)) {
            return TransformSchemaType.STRING;
        }
        if (value.getClass().equals(byte[].class)) {
            return TransformSchemaType.BYTES;
        }
        if (value.getClass().equals(Boolean.class)) {
            return TransformSchemaType.BOOLEAN;
        }
        if (value.getClass().equals(Byte.class)) {
            return TransformSchemaType.INT8;
        }
        if (value.getClass().equals(Short.class)) {
            return TransformSchemaType.INT16;
        }
        if (value.getClass().equals(Integer.class)) {
            return TransformSchemaType.INT32;
        }
        if (value.getClass().equals(Long.class)) {
            return TransformSchemaType.INT64;
        }
        if (value.getClass().equals(Float.class)) {
            return TransformSchemaType.FLOAT;
        }
        if (value.getClass().equals(Double.class)) {
            return TransformSchemaType.DOUBLE;
        }
        if (value.getClass().equals(Date.class)) {
            return TransformSchemaType.DATE;
        }
        if (value.getClass().equals(Timestamp.class)) {
            return TransformSchemaType.TIMESTAMP;
        }
        if (value.getClass().equals(Time.class)) {
            return TransformSchemaType.TIME;
        }
        if (value.getClass().equals(LocalDateTime.class)) {
            return TransformSchemaType.LOCAL_DATE_TIME;
        }
        if (value.getClass().equals(LocalDate.class)) {
            return TransformSchemaType.LOCAL_DATE;
        }
        if (value.getClass().equals(LocalTime.class)) {
            return TransformSchemaType.LOCAL_TIME;
        }
        if (value.getClass().equals(Instant.class)) {
            return TransformSchemaType.INSTANT;
        }
        throw new UnsupportedOperationException("Got an unsupported type: " + value.getClass());
    }

    private ComputeFieldType getFieldType(Object value) {
        if (value == null) {
            throw new UnsupportedOperationException("Cannot get field type from null value");
        }
        if (value instanceof CharSequence) {
            return ComputeFieldType.STRING;
        }
        if (value instanceof ByteBuffer || value.getClass().equals(byte[].class)) {
            return ComputeFieldType.BYTES;
        }
        if (value.getClass().equals(Boolean.class)) {
            return ComputeFieldType.BOOLEAN;
        }
        if (value.getClass().equals(Byte.class)) {
            return ComputeFieldType.INT8;
        }
        if (value.getClass().equals(Short.class)) {
            return ComputeFieldType.INT16;
        }
        if (value.getClass().equals(Integer.class)) {
            return ComputeFieldType.INT32;
        }
        if (value.getClass().equals(Long.class)) {
            return ComputeFieldType.INT64;
        }
        if (value.getClass().equals(Float.class)) {
            return ComputeFieldType.FLOAT;
        }
        if (value.getClass().equals(Double.class)) {
            return ComputeFieldType.DOUBLE;
        }
        if (value.getClass().equals(Date.class)) {
            return ComputeFieldType.DATE;
        }
        if (value.getClass().equals(Timestamp.class)) {
            return ComputeFieldType.TIMESTAMP;
        }
        if (value.getClass().equals(Time.class)) {
            return ComputeFieldType.TIME;
        }
        if (value.getClass().equals(LocalDateTime.class)) {
            return ComputeFieldType.LOCAL_DATE_TIME;
        }
        if (value.getClass().equals(LocalDate.class)) {
            return ComputeFieldType.LOCAL_DATE;
        }
        if (value.getClass().equals(LocalTime.class)) {
            return ComputeFieldType.LOCAL_TIME;
        }
        if (value.getClass().equals(Instant.class)) {
            return ComputeFieldType.INSTANT;
        }
        if (value.getClass().equals(BigDecimal.class)) {
            return ComputeFieldType.DECIMAL;
        }
        if (List.class.isAssignableFrom(value.getClass())) {
            return ComputeFieldType.ARRAY;
        }
        if (Map.class.isAssignableFrom(value.getClass())) {
            return ComputeFieldType.MAP;
        }
        throw new UnsupportedOperationException("Got an unsupported type: " + value.getClass());
    }
}
