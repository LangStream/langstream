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

import ai.langstream.ai.agents.commons.MutableRecord;
import ai.langstream.ai.agents.commons.TransformSchemaType;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

@Builder
public class FlattenStep implements TransformStep {
    // TODO: Cache schema to flattened schema
    // TODO: Microbenchmark the flatten algorithm for performance optimization
    // TODO: Validate flatten delimiter
    // TODO: Add integration test

    public static final String AVRO_READ_OFFSET_PROP = "__AVRO_READ_OFFSET__";

    private static final String DEFAULT_DELIMITER = "_"; // '.' in not valid in AVRO field names

    @Builder.Default private final String delimiter = DEFAULT_DELIMITER;
    private final String part;

    @Override
    public void process(MutableRecord mutableRecord) throws Exception {
        if (part != null && !part.equals("key") && !part.equals("value")) {
            throw new IllegalArgumentException("Unsupported part for Flatten: " + part);
        }
        if ("key".equals(part) || part == null) {
            validateAvro(mutableRecord.getKeySchemaType());
            GenericRecord flattenedKey =
                    flattenGenericRecord((GenericRecord) mutableRecord.getKeyObject());
            mutableRecord.setKeyObject(flattenedKey);
            mutableRecord.setKeyNativeSchema(flattenedKey.getSchema());
        }
        if ("value".equals(part) || part == null) {
            validateAvro(mutableRecord.getValueSchemaType());
            GenericRecord flattenedValue =
                    flattenGenericRecord((GenericRecord) mutableRecord.getValueObject());
            mutableRecord.setValueObject(flattenedValue);
            mutableRecord.setValueNativeSchema(flattenedValue.getSchema());
        }
    }

    void validateAvro(TransformSchemaType schemaType) {
        if (schemaType == null) {
            throw new IllegalStateException("Flatten requires non-null schemas!");
        }

        if (schemaType != TransformSchemaType.AVRO) {
            throw new IllegalStateException(
                    "Unsupported schema type for Flatten: " + schemaType.name());
        }
    }

    GenericRecord flattenGenericRecord(GenericRecord record) {
        List<FieldValuePair> fieldValuePairs = buildFlattenedFields(record);
        List<org.apache.avro.Schema.Field> fields =
                fieldValuePairs.stream().map(FieldValuePair::getField).collect(Collectors.toList());
        org.apache.avro.Schema modified = buildFlattenedSchema(record, fields);
        GenericRecord newRecord = new GenericData.Record(modified);
        fieldValuePairs.forEach(pair -> newRecord.put(pair.getField().name(), pair.getValue()));
        return newRecord;
    }

    private List<FieldValuePair> buildFlattenedFields(GenericRecord record) {
        List<FieldValuePair> flattenedFields = new ArrayList<>();
        org.apache.avro.Schema originalSchema = record.getSchema();
        for (org.apache.avro.Schema.Field field : originalSchema.getFields()) {
            flattenedFields.addAll(
                    flattenField(
                            record, record.getSchema(), field, field.schema().isNullable(), ""));
        }

        return flattenedFields;
    }

    org.apache.avro.Schema buildFlattenedSchema(
            GenericRecord record, List<org.apache.avro.Schema.Field> flattenedFields) {
        org.apache.avro.Schema originalSchema = record.getSchema();
        org.apache.avro.Schema flattenedSchema =
                org.apache.avro.Schema.createRecord(
                        originalSchema.getName(),
                        originalSchema.getDoc(),
                        originalSchema.getNamespace(),
                        false,
                        flattenedFields);
        originalSchema.getObjectProps().entrySet().stream()
                .filter(e -> !AVRO_READ_OFFSET_PROP.equals(e.getKey()))
                .forEach(e -> flattenedSchema.addProp(e.getKey(), e.getValue()));
        return flattenedSchema;
    }

    /**
     * @param record the record that contains the field to be flattened. Each recursive call will
     *     pass the record one level deeper. At any level, the field could become null.
     * @param schema the schema of the record that contains the field to be flattened. Each
     *     recursive call will pass the schema one level deeper. At any level, the schema should
     *     exist.
     * @param field the field to be flattened
     * @param nullable true if the current field schema is nullable, this has to be passed all the
     *     way to the last nested level because anytime one of the ancestors is null, the flattened
     *     field schema could be null even if it is not nullable on the original schema
     * @param flattenedFieldName the field name that is built incrementally with each recursive
     *     call.
     * @return flattened key/value pairs.
     */
    List<FieldValuePair> flattenField(
            @Nullable GenericRecord record,
            org.apache.avro.Schema schema,
            org.apache.avro.Schema.Field field,
            boolean nullable,
            String flattenedFieldName) {
        List<FieldValuePair> flattenedFields = new ArrayList<>();
        // Because of UNION schemas, we cannot tell for sure if the current field is a record, so we
        // have to use reflection
        org.apache.avro.Schema recordSchema = getRecordSchema(schema, field.name());
        if (recordSchema != null) {
            for (org.apache.avro.Schema.Field nestedField : recordSchema.getFields()) {
                GenericRecord genericRecord =
                        record == null ? null : (GenericRecord) record.get(field.name());
                flattenedFields.addAll(
                        flattenField(
                                genericRecord,
                                recordSchema,
                                nestedField,
                                nullable || nestedField.schema().isNullable(),
                                flattenedFieldName + field.name() + delimiter));
            }

            return flattenedFields;
        }

        org.apache.avro.Schema.Field flattenedField =
                createField(field, flattenedFieldName + field.name(), nullable);
        FieldValuePair fieldValuePair =
                new FieldValuePair(
                        flattenedField, record == null ? null : record.get(field.name()));
        flattenedFields.add(fieldValuePair);

        return flattenedFields;
    }

    private org.apache.avro.Schema getRecordSchema(org.apache.avro.Schema schema, String name) {
        if (schema.getType() != org.apache.avro.Schema.Type.RECORD) {
            return null;
        }

        org.apache.avro.Schema fieldSchema = schema.getField(name).schema();
        if (fieldSchema.isUnion()) {
            for (org.apache.avro.Schema s : fieldSchema.getTypes()) {
                if (s.getType() == org.apache.avro.Schema.Type.RECORD) {
                    return s;
                }
            }
        }

        return fieldSchema.getType() == org.apache.avro.Schema.Type.RECORD ? fieldSchema : null;
    }

    /**
     * Creates a new Field instance with the same schema, doc, defaultValue, and order as field has
     * with changing the name to the specified one. It also copies all the props and aliases.
     */
    org.apache.avro.Schema.Field createField(
            org.apache.avro.Schema.Field field, String name, boolean nullable) {
        org.apache.avro.Schema newSchema = field.schema();
        if (nullable && !newSchema.isNullable()) {
            newSchema = SchemaBuilder.unionOf().nullType().and().type(newSchema).endUnion();
        }
        org.apache.avro.Schema.Field newField =
                new org.apache.avro.Schema.Field(
                        name, newSchema, field.doc(), field.defaultVal(), field.order());
        newField.putAll(field);
        field.aliases().forEach(newField::addAlias);

        return newField;
    }

    @Value
    private static class FieldValuePair {
        org.apache.avro.Schema.Field field;
        Object value;
    }
}
