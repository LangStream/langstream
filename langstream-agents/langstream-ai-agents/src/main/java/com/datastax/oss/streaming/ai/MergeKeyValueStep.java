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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class MergeKeyValueStep implements TransformStep {
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final Map<org.apache.avro.Schema, Map<org.apache.avro.Schema, org.apache.avro.Schema>>
            schemaCache = new ConcurrentHashMap<>();

    @Override
    public void process(MutableRecord mutableRecord) {
        TransformSchemaType keySchemaType = mutableRecord.getKeySchemaType();
        if (keySchemaType == null) {
            return;
        }
        Object keyObject = mutableRecord.getKeyObject();
        Object valueObject = mutableRecord.getValueObject();
        if (keyObject instanceof Map && valueObject instanceof Map) {
            Map<Object, Object> value = (Map<Object, Object>) valueObject;
            Map<String, Object> keyCopy =
                    OBJECT_MAPPER.convertValue(keyObject, new TypeReference<>() {});
            keyCopy.forEach(value::putIfAbsent);
        } else if (keySchemaType == TransformSchemaType.AVRO
                && mutableRecord.getValueSchemaType() == TransformSchemaType.AVRO) {
            GenericRecord avroKeyRecord = (GenericRecord) keyObject;
            org.apache.avro.Schema avroKeySchema = avroKeyRecord.getSchema();

            GenericRecord avroValueRecord = (GenericRecord) valueObject;
            org.apache.avro.Schema avroValueSchema = avroValueRecord.getSchema();

            org.apache.avro.Schema mergedSchema = getMergedSchema(avroKeySchema, avroValueSchema);
            GenericRecord newRecord = new GenericData.Record(mergedSchema);
            avroValueSchema
                    .getFields()
                    .forEach(
                            field ->
                                    newRecord.put(field.name(), avroValueRecord.get(field.name())));
            for (org.apache.avro.Schema.Field field : avroKeySchema.getFields()) {
                if (avroValueSchema.getField(field.name()) == null) {
                    newRecord.put(field.name(), avroKeyRecord.get(field.name()));
                }
            }
            mutableRecord.setValueObject(newRecord);
            mutableRecord.setValueNativeSchema(newRecord.getSchema());
        } else if (keySchemaType == TransformSchemaType.JSON
                && mutableRecord.getValueSchemaType() == TransformSchemaType.JSON) {
            org.apache.avro.Schema avroKeySchema =
                    (org.apache.avro.Schema) mutableRecord.getKeyNativeSchema();
            org.apache.avro.Schema avroValueSchema =
                    (org.apache.avro.Schema) mutableRecord.getValueNativeSchema();
            if (avroValueSchema != null && avroKeySchema != null) {
                org.apache.avro.Schema mergedSchema =
                        getMergedSchema(avroKeySchema, avroValueSchema);
                mutableRecord.setValueNativeSchema(mergedSchema);
            } else {
                mutableRecord.setValueNativeSchema(null);
            }
            ObjectNode newValue = ((ObjectNode) keyObject).deepCopy();
            newValue.setAll(((ObjectNode) valueObject).deepCopy());
            mutableRecord.setValueObject(newValue);
        }
    }

    private org.apache.avro.Schema getMergedSchema(
            org.apache.avro.Schema avroKeySchema, org.apache.avro.Schema avroValueSchema) {
        List<org.apache.avro.Schema.Field> fields =
                avroKeySchema.getFields().stream()
                        .filter(field -> avroValueSchema.getField(field.name()) == null)
                        .map(
                                f ->
                                        new org.apache.avro.Schema.Field(
                                                f.name(),
                                                f.schema(),
                                                f.doc(),
                                                f.defaultVal(),
                                                f.order()))
                        .collect(Collectors.toList());
        fields.addAll(
                avroValueSchema.getFields().stream()
                        .map(
                                f ->
                                        new org.apache.avro.Schema.Field(
                                                f.name(),
                                                f.schema(),
                                                f.doc(),
                                                f.defaultVal(),
                                                f.order()))
                        .collect(Collectors.toList()));

        Map<org.apache.avro.Schema, org.apache.avro.Schema> schemaCacheKey =
                schemaCache.computeIfAbsent(avroKeySchema, s -> new ConcurrentHashMap<>());
        return schemaCacheKey.computeIfAbsent(
                avroValueSchema,
                schema ->
                        org.apache.avro.Schema.createRecord(
                                avroValueSchema.getName(),
                                null,
                                avroValueSchema.getNamespace(),
                                false,
                                fields));
    }
}
