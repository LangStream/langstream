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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

public class AvroUtil {

    /**
     * Returns the logical type of the schema. If the schema is a union, it will return the logical
     * type of any of the union types.
     */
    public static LogicalType getLogicalType(Schema schema) {
        if (!schema.isUnion()) {
            return schema.getLogicalType();
        }

        return schema.getTypes().stream()
                .map(Schema::getLogicalType)
                .filter(Objects::nonNull)
                .findAny()
                .orElse(null);
    }

    public static GenericRecord dropAvroRecordFields(
            GenericRecord record,
            Collection<String> fields,
            Map<org.apache.avro.Schema, org.apache.avro.Schema> schemaCache) {
        org.apache.avro.Schema avroSchema = record.getSchema();
        if (schemaCache.get(avroSchema) != null
                || fields.stream().anyMatch(field -> avroSchema.getField(field) != null)) {
            org.apache.avro.Schema modified = dropAvroSchemaFields(avroSchema, fields, schemaCache);
            GenericRecord newRecord = new GenericData.Record(modified);
            for (org.apache.avro.Schema.Field field : modified.getFields()) {
                newRecord.put(field.name(), record.get(field.name()));
            }
            return newRecord;
        }
        return record;
    }

    public static Schema dropAvroSchemaFields(
            Schema avroSchema, Collection<String> fields, Map<Schema, Schema> schemaCache) {
        return schemaCache.computeIfAbsent(
                avroSchema,
                schema ->
                        Schema.createRecord(
                                avroSchema.getName(),
                                avroSchema.getDoc(),
                                avroSchema.getNamespace(),
                                avroSchema.isError(),
                                avroSchema.getFields().stream()
                                        .filter(f -> !fields.contains(f.name()))
                                        .map(
                                                f ->
                                                        new Schema.Field(
                                                                f.name(),
                                                                f.schema(),
                                                                f.doc(),
                                                                f.defaultVal(),
                                                                f.order()))
                                        .collect(Collectors.toList())));
    }

    public static GenericData.Record addOrReplaceAvroRecordFields(
            GenericRecord record,
            Map<Schema.Field, Object> newFields,
            Map<Schema, Schema> schemaCache) {
        Schema newSchema =
                addOrReplaceAvroSchemaFields(record.getSchema(), newFields.keySet(), schemaCache);
        GenericRecordBuilder newRecordBuilder = new GenericRecordBuilder(newSchema);
        for (Schema.Field f : newSchema.getFields()) {
            if (newFields.containsKey(f)) {
                Object value = newFields.get(f);
                if ((value instanceof Collection) && !(value instanceof GenericArray)) {
                    value = new GenericData.Array<>(f.schema(), (Collection<Object>) value);
                }
                newRecordBuilder.set(f.name(), value);
            } else {
                newRecordBuilder.set(f.name(), record.get(f.name()));
            }
        }
        return newRecordBuilder.build();
    }

    public static Schema addOrReplaceAvroSchemaFields(
            Schema avroSchema,
            Collection<Schema.Field> newFields,
            Map<Schema, Schema> schemaCache) {
        Map<String, Schema.Field> newFieldsByName = new LinkedHashMap<>();
        newFields.forEach(k -> newFieldsByName.put(k.name(), k));

        // allFields is the intersection between existing fields and computed fields. Computed
        // fields
        // take precedence.
        List<Schema.Field> allFields = new ArrayList<>();
        for (Schema.Field f : avroSchema.getFields()) {
            if (newFieldsByName.containsKey(f.name())) {
                allFields.add(newFieldsByName.get(f.name()));
                newFieldsByName.remove(f.name());
            } else {
                allFields.add(
                        new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal(), f.order()));
            }
        }
        allFields.addAll(newFieldsByName.values());
        return schemaCache.computeIfAbsent(
                avroSchema,
                schema ->
                        Schema.createRecord(
                                avroSchema.getName(),
                                avroSchema.getDoc(),
                                avroSchema.getNamespace(),
                                avroSchema.isError(),
                                allFields));
    }
}
