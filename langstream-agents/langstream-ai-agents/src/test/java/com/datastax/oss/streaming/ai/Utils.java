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

import static ai.langstream.ai.agents.commons.MutableRecord.attemptJsonConversion;
import static com.datastax.oss.streaming.ai.FlattenStep.AVRO_READ_OFFSET_PROP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.langstream.ai.agents.commons.MutableRecord;
import ai.langstream.ai.agents.commons.TransformSchemaType;
import com.datastax.oss.pulsar.functions.transforms.JsonNodeSchema;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.client.impl.schema.SchemaInfoImpl;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroRecord;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.api.utils.FunctionRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
public class Utils {

    @SneakyThrows
    public static <T> Record<T> process(Record<GenericObject> record, TransformStep step) {
        Utils.TestContext context = new Utils.TestContext(record, new HashMap<>());
        MutableRecord mutableRecord =
                newTransformContext(context, record.getValue().getNativeObject());
        try {
            step.processAsync(mutableRecord).get();
        } catch (ExecutionException error) {
            throw error.getCause();
        }
        return send(context, mutableRecord);
    }

    public static GenericData.Record getRecord(Schema<?> schema, byte[] value) throws IOException {
        DatumReader<GenericData.Record> reader =
                new GenericDatumReader<>(
                        (org.apache.avro.Schema) schema.getNativeSchema().orElseThrow());
        Decoder decoder = DecoderFactory.get().binaryDecoder(value, null);
        return reader.read(null, decoder);
    }

    public static Record<GenericObject> createTestAvroKeyValueRecord() {
        return createTestStructKeyValueRecord(SchemaType.AVRO);
    }

    public static Record<GenericObject> createTestJsonKeyValueRecord() {
        return createTestStructKeyValueRecord(SchemaType.JSON);
    }

    public static Record<GenericObject> createTestStructKeyValueRecord(SchemaType schemaType) {
        RecordSchemaBuilder keySchemaBuilder =
                org.apache.pulsar.client.api.schema.SchemaBuilder.record("record");
        keySchemaBuilder.field("keyField1").type(SchemaType.STRING);
        keySchemaBuilder.field("keyField2").type(SchemaType.STRING);
        keySchemaBuilder.field("keyField3").type(SchemaType.STRING);
        GenericSchema<GenericRecord> keySchema = Schema.generic(keySchemaBuilder.build(schemaType));

        RecordSchemaBuilder valueSchemaBuilder =
                org.apache.pulsar.client.api.schema.SchemaBuilder.record("record");
        valueSchemaBuilder.field("valueField1").type(SchemaType.STRING);
        valueSchemaBuilder.field("valueField2").type(SchemaType.STRING);
        valueSchemaBuilder.field("valueField3").type(SchemaType.STRING);
        GenericSchema<GenericRecord> valueSchema =
                Schema.generic(valueSchemaBuilder.build(schemaType));

        GenericRecord keyRecord =
                keySchema
                        .newRecordBuilder()
                        .set("keyField1", "key1")
                        .set("keyField2", "key2")
                        .set("keyField3", "key3")
                        .build();

        GenericRecord valueRecord =
                valueSchema
                        .newRecordBuilder()
                        .set("valueField1", "value1")
                        .set("valueField2", "value2")
                        .set("valueField3", "value3")
                        .build();

        Schema<KeyValue<GenericRecord, GenericRecord>> keyValueSchema =
                Schema.KeyValue(keySchema, valueSchema, KeyValueEncodingType.SEPARATED);

        KeyValue<GenericRecord, GenericRecord> keyValue = new KeyValue<>(keyRecord, valueRecord);

        GenericObject genericObject =
                new GenericObject() {
                    @Override
                    public SchemaType getSchemaType() {
                        return SchemaType.KEY_VALUE;
                    }

                    @Override
                    public Object getNativeObject() {
                        return keyValue;
                    }
                };

        return new TestRecord<>(keyValueSchema, genericObject, null);
    }

    public static Record<GenericObject> createNestedAvroRecord(int levels, String key) {
        GenericAvroRecord valueRecord = createNestedAvroRecord(levels);

        Schema<org.apache.avro.generic.GenericRecord> pulsarValueSchema =
                new Utils.NativeSchemaWrapper(
                        valueRecord.getAvroRecord().getSchema(), SchemaType.AVRO);

        return new Utils.TestRecord<>(pulsarValueSchema, valueRecord, key);
    }

    public static Record<GenericObject> createNestedJSONRecord(int levels, String key) {
        GenericAvroRecord valueRecord = createNestedAvroRecord(levels);

        Schema<org.apache.avro.generic.GenericRecord> pulsarValueSchema =
                new Utils.NativeSchemaWrapper(
                        valueRecord.getAvroRecord().getSchema(), SchemaType.JSON);

        return new Utils.TestRecord<>(pulsarValueSchema, valueRecord, key);
    }

    public static Record<GenericObject> createNestedAvroKeyValueRecord(int levels) {
        GenericAvroRecord keyRecord = createNestedAvroRecord(levels);
        GenericAvroRecord valueRecord = createNestedAvroRecord(levels);

        GenericObject genericObject =
                new GenericObject() {
                    @Override
                    public SchemaType getSchemaType() {
                        return SchemaType.KEY_VALUE;
                    }

                    @Override
                    public Object getNativeObject() {
                        return new KeyValue<>(keyRecord, valueRecord);
                    }
                };

        Schema<org.apache.avro.generic.GenericRecord> pulsarKeySchema =
                new Utils.NativeSchemaWrapper(
                        keyRecord.getAvroRecord().getSchema(), SchemaType.AVRO);
        Schema<org.apache.avro.generic.GenericRecord> pulsarValueSchema =
                new Utils.NativeSchemaWrapper(
                        valueRecord.getAvroRecord().getSchema(), SchemaType.AVRO);

        Schema<
                        KeyValue<
                                org.apache.avro.generic.GenericRecord,
                                org.apache.avro.generic.GenericRecord>>
                keyValueSchema =
                        Schema.KeyValue(
                                pulsarKeySchema, pulsarValueSchema, KeyValueEncodingType.SEPARATED);
        Map<String, String> props = new HashMap<>();
        props.put("p1", "v1");
        props.put("p2", "v2");

        return TestRecord.<GenericObject>builder()
                .schema(keyValueSchema)
                .value(genericObject)
                .key("key1")
                .topicName("topic-1")
                .destinationTopic("dest-topic-1")
                .eventTime(1662493532L)
                .properties(props)
                .build();
    }

    /**
     * Returns a nested Avro record with a certain number of levels. Example for levels = 4: {
     * "level1KeyField1": "level1_Key1", "level1KeyField2": { "level2KeyField1": "level2_Key1",
     * "level2KeyField2": { "level3KeyField1": "level3_Key1", "level3KeyField2": {
     * "level4KeyField1": "level4_Key1", "level4KeyField2": "level4_Key2" } } } }
     */
    public static GenericAvroRecord createNestedAvroRecord(int levels) {
        // Create the last, unnested level
        List<org.apache.avro.Schema.Field> fields = new ArrayList<>();
        org.apache.avro.Schema nullAndString =
                SchemaBuilder.unionOf().stringType().and().nullType().endUnion();
        org.apache.avro.Schema nullLevel =
                org.apache.avro.Schema.createRecord(
                        "nullLevel",
                        "doc ",
                        "ns",
                        false,
                        List.of(
                                new org.apache.avro.Schema.Field(
                                        "ignored",
                                        org.apache.avro.Schema.create(
                                                org.apache.avro.Schema.Type.STRING))));
        org.apache.avro.Schema nullAndRecord =
                SchemaBuilder.unionOf().nullType().and().type(nullLevel).endUnion();
        fields.add(
                new org.apache.avro.Schema.Field(
                        "level" + levels + "String",
                        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
                        "stringDoc",
                        "stringDefault"));
        fields.add(
                new org.apache.avro.Schema.Field(
                        "level" + levels + "Integer",
                        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT),
                        "integerDoc",
                        3));
        fields.add(
                new org.apache.avro.Schema.Field(
                        "level" + levels + "Double",
                        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE),
                        "doubleDoc",
                        5.5D));
        org.apache.avro.Schema arraySchema =
                org.apache.avro.Schema.createArray(
                        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING));
        fields.add(
                new org.apache.avro.Schema.Field(
                        "level" + levels + "Array",
                        arraySchema,
                        "arrayDoc",
                        Collections.emptyList()));
        org.apache.avro.Schema.Field stringWithProps =
                new org.apache.avro.Schema.Field(
                        "level" + levels + "StringWithPropsAndAlias",
                        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING));
        stringWithProps.addProp("p1", "v1");
        stringWithProps.addProp("p2", "v2");
        stringWithProps.addProp(AVRO_READ_OFFSET_PROP, "5");
        stringWithProps.addAlias("a1");
        fields.add(stringWithProps);
        fields.add(new org.apache.avro.Schema.Field("level" + levels + "Union", nullAndString));
        fields.add(new org.apache.avro.Schema.Field("level" + levels + "Null", nullAndString));
        fields.add(
                new org.apache.avro.Schema.Field("level" + levels + "NullRecord", nullAndRecord));
        org.apache.avro.Schema lastLevelSchema =
                org.apache.avro.Schema.createRecord("nested" + levels, "doc ", "ns", false, fields);
        org.apache.avro.generic.GenericRecord lastLevelRecord =
                new GenericData.Record(lastLevelSchema);
        lastLevelRecord.put("level" + levels + "String", "level" + levels + "_" + "1");
        lastLevelRecord.put("level" + levels + "Integer", 9);
        lastLevelRecord.put("level" + levels + "Double", 8.8D);
        lastLevelRecord.put(
                "level" + levels + "Array",
                List.of("level" + levels + "_" + "1", "level" + levels + "_" + "2"));
        lastLevelRecord.put(
                "level" + levels + "StringWithPropsAndAlias", "level" + levels + "_" + "WithProps");
        lastLevelRecord.put("level" + levels + "Union", "level" + levels + "_" + "2");
        lastLevelRecord.put("level" + levels + "Null", null);
        lastLevelRecord.put("level" + levels + "NullRecord", null);

        org.apache.avro.Schema nextLevelSchema = lastLevelSchema;
        org.apache.avro.generic.GenericRecord nextLevelRecord = lastLevelRecord;
        // Create the nested levels one by one, working backwards from the last level up to the top
        // level
        for (int level = levels - 1; level > 0; level--) {
            fields = new ArrayList<>();
            fields.add(
                    new org.apache.avro.Schema.Field(
                            "level" + level + "String",
                            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING)));
            fields.add(
                    new org.apache.avro.Schema.Field("level" + level + "Record", nextLevelSchema));
            org.apache.avro.Schema currentLevelSchema =
                    org.apache.avro.Schema.createRecord(
                            "nested" + level, "doc ", "ns", false, fields);

            org.apache.avro.generic.GenericRecord currentLevelRecord =
                    new GenericData.Record(currentLevelSchema);
            currentLevelRecord.put("level" + level + "String", "level" + level + "_" + "1");
            currentLevelRecord.put("level" + level + "Record", nextLevelRecord);

            nextLevelSchema = currentLevelSchema;
            nextLevelRecord = currentLevelRecord;
        }

        nextLevelSchema.addProp(AVRO_READ_OFFSET_PROP, 5);
        nextLevelSchema.addProp("t1", 9);
        List<Field> pulsarFields =
                fields.stream().map(v -> new Field(v.name(), v.pos())).collect(Collectors.toList());
        return new GenericAvroRecord(new byte[0], nextLevelSchema, pulsarFields, nextLevelRecord);
    }

    public static MutableRecord createContextWithPrimitiveRecord(
            Schema<?> schema, Object value, String key) {
        Record<GenericObject> record =
                new Utils.TestRecord<>(
                        schema,
                        AutoConsumeSchema.wrapPrimitiveObject(
                                value, schema.getSchemaInfo().getType(), new byte[] {}),
                        key);
        return newTransformContext(
                new Utils.TestContext(record, new HashMap<>()),
                record.getValue().getNativeObject());
    }

    static void assertOptionalField(
            GenericData.Record record,
            String fieldName,
            org.apache.avro.Schema.Type expectedType,
            Object expectedValue) {
        assertTrue(record.hasField(fieldName));
        org.apache.avro.Schema.Field field = record.getSchema().getField(fieldName);
        assertTrue(field.schema().isNullable());
        assertEquals(field.defaultVal(), org.apache.avro.Schema.NULL_VALUE);
        assertTrue(field.schema().isUnion());
        org.apache.avro.Schema nullSchema = field.schema().getTypes().get(0);
        assertEquals(nullSchema.getType(), org.apache.avro.Schema.Type.NULL);
        org.apache.avro.Schema typedSchema = field.schema().getTypes().get(1);
        assertEquals(typedSchema.getType(), expectedType);
        assertEquals(record.get(fieldName), expectedValue);
    }

    static void assertNonOptionalField(
            GenericData.Record record,
            String fieldName,
            org.apache.avro.Schema.Type expectedType,
            Object expectedValue) {
        assertTrue(record.hasField(fieldName));
        org.apache.avro.Schema.Field field = record.getSchema().getField(fieldName);
        assertFalse(field.schema().isNullable());
        assertNull(field.defaultVal());
        assertFalse(field.schema().isUnion());
        assertEquals(field.schema().getType(), expectedType);
        assertEquals(record.get(fieldName), expectedValue);
    }

    public static MutableRecord newTransformContext(Context context, Object value) {
        return newTransformContext(context, value, true);
    }

    public static MutableRecord newTransformContext(
            Context context, Object value, boolean attemptJsonConversion) {
        Record<?> currentRecord = context.getCurrentRecord();
        MutableRecord mutableRecord = new MutableRecord();
        mutableRecord.setProperties(new HashMap<>());
        mutableRecord.setInputTopic(currentRecord.getTopicName().orElse(null));
        mutableRecord.setOutputTopic(currentRecord.getDestinationTopic().orElse(null));
        mutableRecord.setKey(currentRecord.getKey().orElse(null));
        mutableRecord.setEventTime(currentRecord.getEventTime().orElse(null));

        if (currentRecord.getProperties() != null) {
            mutableRecord.setProperties(new HashMap<>(currentRecord.getProperties()));
        }

        Schema<?> schema = currentRecord.getSchema();
        if (schema instanceof KeyValueSchema && value instanceof KeyValue) {
            KeyValueSchema<?, ?> kvSchema = (KeyValueSchema<?, ?>) schema;
            KeyValue<?, ?> kv = (KeyValue<?, ?>) value;
            Schema<?> keySchema = kvSchema.getKeySchema();
            Schema<?> valueSchema = kvSchema.getValueSchema();
            mutableRecord.setKeySchemaType(pulsarSchemaToTransformSchemaType(keySchema));
            mutableRecord.setKeyNativeSchema(getNativeSchema(keySchema));
            mutableRecord.setKeyObject(
                    keySchema.getSchemaInfo().getType().isStruct()
                            ? ((GenericObject) kv.getKey()).getNativeObject()
                            : kv.getKey());
            mutableRecord.setValueSchemaType(pulsarSchemaToTransformSchemaType(valueSchema));
            mutableRecord.setValueNativeSchema(getNativeSchema(valueSchema));
            mutableRecord.setValueObject(
                    valueSchema.getSchemaInfo().getType().isStruct()
                            ? ((GenericObject) kv.getValue()).getNativeObject()
                            : kv.getValue());
            mutableRecord
                    .getCustomContext()
                    .put("keyValueEncodingType", kvSchema.getKeyValueEncodingType());
        } else {
            mutableRecord.setValueSchemaType(pulsarSchemaToTransformSchemaType(schema));
            mutableRecord.setValueNativeSchema(getNativeSchema(schema));
            mutableRecord.setValueObject(value);
        }
        if (attemptJsonConversion) {
            mutableRecord.setKeyObject(attemptJsonConversion(mutableRecord.getKeyObject()));
            mutableRecord.setValueObject(attemptJsonConversion(mutableRecord.getValueObject()));
        }
        return mutableRecord;
    }

    private static Object getNativeSchema(Schema<?> schema) {
        if (schema == null) {
            return null;
        }
        return schema.getNativeSchema().orElse(null);
    }

    private static TransformSchemaType pulsarSchemaToTransformSchemaType(Schema<?> schema) {
        if (schema == null) {
            return null;
        }
        switch (schema.getSchemaInfo().getType()) {
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
            case STRING:
                return TransformSchemaType.STRING;
            case BYTES:
                return TransformSchemaType.BYTES;
            case DATE:
                return TransformSchemaType.DATE;
            case TIME:
                return TransformSchemaType.TIME;
            case TIMESTAMP:
                return TransformSchemaType.TIMESTAMP;
            case INSTANT:
                return TransformSchemaType.INSTANT;
            case LOCAL_DATE:
                return TransformSchemaType.LOCAL_DATE;
            case LOCAL_TIME:
                return TransformSchemaType.LOCAL_TIME;
            case LOCAL_DATE_TIME:
                return TransformSchemaType.LOCAL_DATE_TIME;
            case JSON:
                return TransformSchemaType.JSON;
            case AVRO:
                return TransformSchemaType.AVRO;
            case PROTOBUF:
                return TransformSchemaType.PROTOBUF;
            default:
                throw new IllegalArgumentException(
                        "Unsupported schema type " + schema.getSchemaInfo().getType());
        }
    }

    public static <T> Record<T> send(Context context, MutableRecord mutableRecord)
            throws IOException {
        if (mutableRecord.isDropCurrentRecord()) {
            return null;
        }
        mutableRecord.convertAvroToBytes();
        mutableRecord.convertMapToStringOrBytes();

        Schema outputSchema;
        Object outputObject;
        if (mutableRecord.getKeySchemaType() != null) {
            KeyValueEncodingType keyValueEncodingType =
                    (KeyValueEncodingType)
                            mutableRecord.getCustomContext().get("keyValueEncodingType");
            outputSchema =
                    Schema.KeyValue(
                            buildSchema(
                                    mutableRecord.getKeySchemaType(),
                                    mutableRecord.getKeyNativeSchema()),
                            buildSchema(
                                    mutableRecord.getValueSchemaType(),
                                    mutableRecord.getValueNativeSchema()),
                            keyValueEncodingType != null
                                    ? keyValueEncodingType
                                    : KeyValueEncodingType.INLINE);
            Object outputKeyObject = mutableRecord.getKeyObject();
            Object outputValueObject = mutableRecord.getValueObject();
            outputObject = new KeyValue<>(outputKeyObject, outputValueObject);
        } else {
            outputSchema =
                    buildSchema(
                            mutableRecord.getValueSchemaType(),
                            mutableRecord.getValueNativeSchema());
            outputObject = mutableRecord.getValueObject();
        }

        if (log.isDebugEnabled()) {
            log.debug("output {} schema {}", outputObject, outputSchema);
        }

        FunctionRecord.FunctionRecordBuilder<T> recordBuilder =
                context.newOutputRecordBuilder(outputSchema)
                        .destinationTopic(mutableRecord.getOutputTopic())
                        .value(outputObject)
                        .properties(mutableRecord.getProperties());

        if (mutableRecord.getKeySchemaType() == null && mutableRecord.getKey() != null) {
            recordBuilder.key(mutableRecord.getKey());
        }

        return recordBuilder.build();
    }

    private static Schema<?> buildSchema(TransformSchemaType schemaType, Object nativeSchema) {
        if (schemaType == null) {
            throw new IllegalArgumentException("Schema type should not be null.");
        }
        switch (schemaType) {
            case INT8:
                return Schema.INT8;
            case INT16:
                return Schema.INT16;
            case INT32:
                return Schema.INT32;
            case INT64:
                return Schema.INT64;
            case FLOAT:
                return Schema.FLOAT;
            case DOUBLE:
                return Schema.DOUBLE;
            case BOOLEAN:
                return Schema.BOOL;
            case STRING:
                return Schema.STRING;
            case BYTES:
                return Schema.BYTES;
            case DATE:
                return Schema.DATE;
            case TIME:
                return Schema.TIME;
            case TIMESTAMP:
                return Schema.TIMESTAMP;
            case INSTANT:
                return Schema.INSTANT;
            case LOCAL_DATE:
                return Schema.LOCAL_DATE;
            case LOCAL_TIME:
                return Schema.LOCAL_TIME;
            case LOCAL_DATE_TIME:
                return Schema.LOCAL_DATE_TIME;
            case AVRO:
                return Schema.NATIVE_AVRO(nativeSchema);
            case JSON:
                return new JsonNodeSchema((org.apache.avro.Schema) nativeSchema);
            default:
                throw new IllegalArgumentException("Unsupported schema type " + schemaType);
        }
    }

    @Builder
    @RequiredArgsConstructor
    @AllArgsConstructor
    public static class TestRecord<T> implements Record<T> {
        private final Schema<?> schema;
        private final T value;
        private final String key;
        private String topicName;
        private String destinationTopic;
        private Long eventTime;
        Map<String, String> properties;

        @Override
        public Optional<String> getKey() {
            return Optional.ofNullable(key);
        }

        @Override
        public Schema<T> getSchema() {
            return (Schema<T>) schema;
        }

        @Override
        public T getValue() {
            return value;
        }

        @Override
        public Optional<String> getTopicName() {
            return Optional.ofNullable(topicName);
        }

        @Override
        public Optional<String> getDestinationTopic() {
            return Optional.ofNullable(destinationTopic);
        }

        @Override
        public Optional<Long> getEventTime() {
            return Optional.ofNullable(eventTime);
        }

        @Override
        public Map<String, String> getProperties() {
            return properties;
        }
    }

    public static class TestContext implements Context {
        private final Record<?> currentRecord;
        private final Map<String, Object> userConfig;

        public TestContext(Record<?> currentRecord, Map<String, Object> userConfig) {
            this.currentRecord = currentRecord;
            this.userConfig = userConfig;
        }

        @Override
        public Collection<String> getInputTopics() {
            return null;
        }

        @Override
        public String getOutputTopic() {
            return "test-context-topic";
        }

        @Override
        public Record<?> getCurrentRecord() {
            return currentRecord;
        }

        @Override
        public String getOutputSchemaType() {
            return null;
        }

        @Override
        public String getFunctionName() {
            return null;
        }

        @Override
        public String getFunctionId() {
            return null;
        }

        @Override
        public String getFunctionVersion() {
            return null;
        }

        @Override
        public Map<String, Object> getUserConfigMap() {
            return userConfig;
        }

        @Override
        public Optional<Object> getUserConfigValue(String key) {
            return Optional.ofNullable(userConfig.get(key));
        }

        @Override
        public Object getUserConfigValueOrDefault(String key, Object defaultValue) {
            return null;
        }

        @Override
        public PulsarAdmin getPulsarAdmin() {
            return null;
        }

        @Override
        public <X> CompletableFuture<Void> publish(
                String topicName, X object, String schemaOrSerdeClassName) {
            return null;
        }

        @Override
        public <X> CompletableFuture<Void> publish(String topicName, X object) {
            return null;
        }

        @Override
        public <X> TypedMessageBuilder<X> newOutputMessage(String topicName, Schema<X> schema) {
            return null;
        }

        @Override
        public <X> ConsumerBuilder<X> newConsumerBuilder(Schema<X> schema) {
            return null;
        }

        @Override
        public <X> FunctionRecord.FunctionRecordBuilder<X> newOutputRecordBuilder(
                Schema<X> schema) {
            return FunctionRecord.from(
                    new Context() {
                        @Override
                        public Collection<String> getInputTopics() {
                            return null;
                        }

                        @Override
                        public String getOutputTopic() {
                            return "test-context-topic";
                        }

                        @Override
                        public Record<?> getCurrentRecord() {
                            return currentRecord;
                        }

                        @Override
                        public String getOutputSchemaType() {
                            return null;
                        }

                        @Override
                        public String getFunctionName() {
                            return null;
                        }

                        @Override
                        public String getFunctionId() {
                            return null;
                        }

                        @Override
                        public String getFunctionVersion() {
                            return null;
                        }

                        @Override
                        public Map<String, Object> getUserConfigMap() {
                            return null;
                        }

                        @Override
                        public Optional<Object> getUserConfigValue(String key) {
                            return Optional.empty();
                        }

                        @Override
                        public Object getUserConfigValueOrDefault(String key, Object defaultValue) {
                            return null;
                        }

                        @Override
                        public PulsarAdmin getPulsarAdmin() {
                            return null;
                        }

                        @Override
                        public <O> CompletableFuture<Void> publish(
                                String topicName, O object, String schemaOrSerdeClassName) {
                            return null;
                        }

                        @Override
                        public <O> CompletableFuture<Void> publish(String topicName, O object) {
                            return null;
                        }

                        @Override
                        public <O> TypedMessageBuilder<O> newOutputMessage(
                                String topicName, Schema<O> schema) {
                            return null;
                        }

                        @Override
                        public <O> ConsumerBuilder<O> newConsumerBuilder(Schema<O> schema) {
                            return null;
                        }

                        @Override
                        public <O> FunctionRecord.FunctionRecordBuilder<O> newOutputRecordBuilder(
                                Schema<O> schema) {
                            return null;
                        }

                        @Override
                        public String getTenant() {
                            return null;
                        }

                        @Override
                        public String getNamespace() {
                            return null;
                        }

                        @Override
                        public int getInstanceId() {
                            return 0;
                        }

                        @Override
                        public int getNumInstances() {
                            return 0;
                        }

                        @Override
                        public Logger getLogger() {
                            return LoggerFactory.getILoggerFactory().getLogger("Context");
                        }

                        @Override
                        public String getSecret(String secretName) {
                            return null;
                        }

                        @Override
                        public void putState(String key, ByteBuffer value) {}

                        @Override
                        public CompletableFuture<Void> putStateAsync(String key, ByteBuffer value) {
                            return null;
                        }

                        @Override
                        public ByteBuffer getState(String key) {
                            return null;
                        }

                        @Override
                        public CompletableFuture<ByteBuffer> getStateAsync(String key) {
                            return null;
                        }

                        @Override
                        public void deleteState(String key) {}

                        @Override
                        public CompletableFuture<Void> deleteStateAsync(String key) {
                            return null;
                        }

                        @Override
                        public void incrCounter(String key, long amount) {}

                        @Override
                        public CompletableFuture<Void> incrCounterAsync(String key, long amount) {
                            return null;
                        }

                        @Override
                        public long getCounter(String key) {
                            return 0;
                        }

                        @Override
                        public CompletableFuture<Long> getCounterAsync(String key) {
                            return null;
                        }

                        @Override
                        public void recordMetric(String metricName, double value) {}
                    },
                    schema);
        }

        @Override
        public String getTenant() {
            return null;
        }

        @Override
        public String getNamespace() {
            return null;
        }

        @Override
        public int getInstanceId() {
            return 0;
        }

        @Override
        public int getNumInstances() {
            return 0;
        }

        @Override
        public Logger getLogger() {
            return LoggerFactory.getILoggerFactory().getLogger("Context");
        }

        @Override
        public String getSecret(String secretName) {
            return null;
        }

        @Override
        public void putState(String key, ByteBuffer value) {}

        @Override
        public CompletableFuture<Void> putStateAsync(String key, ByteBuffer value) {
            return null;
        }

        @Override
        public ByteBuffer getState(String key) {
            return null;
        }

        @Override
        public CompletableFuture<ByteBuffer> getStateAsync(String key) {
            return null;
        }

        @Override
        public void deleteState(String key) {}

        @Override
        public CompletableFuture<Void> deleteStateAsync(String key) {
            return null;
        }

        @Override
        public void incrCounter(String key, long amount) {}

        @Override
        public CompletableFuture<Void> incrCounterAsync(String key, long amount) {
            return null;
        }

        @Override
        public long getCounter(String key) {
            return 0;
        }

        @Override
        public CompletableFuture<Long> getCounterAsync(String key) {
            return null;
        }

        @Override
        public void recordMetric(String metricName, double value) {}
    }

    public static class NativeSchemaWrapper
            implements org.apache.pulsar.client.api.Schema<org.apache.avro.generic.GenericRecord> {

        private final SchemaInfo pulsarSchemaInfo;
        private final org.apache.avro.Schema nativeSchema;

        private final SchemaType pulsarSchemaType;

        private final SpecificDatumWriter<org.apache.avro.generic.GenericRecord> datumWriter;

        public NativeSchemaWrapper(
                org.apache.avro.Schema nativeSchema, SchemaType pulsarSchemaType) {
            this.nativeSchema = nativeSchema;
            this.pulsarSchemaType = pulsarSchemaType;
            this.pulsarSchemaInfo =
                    SchemaInfoImpl.builder()
                            .schema(nativeSchema.toString(false).getBytes(StandardCharsets.UTF_8))
                            .properties(new HashMap<>())
                            .type(pulsarSchemaType)
                            .name(nativeSchema.getName())
                            .build();
            this.datumWriter = new SpecificDatumWriter<>(this.nativeSchema);
        }

        @Override
        public byte[] encode(org.apache.avro.generic.GenericRecord genericRecord) {
            try {

                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                BinaryEncoder binaryEncoder =
                        new EncoderFactory().binaryEncoder(byteArrayOutputStream, null);
                datumWriter.write(genericRecord, binaryEncoder);
                binaryEncoder.flush();
                return byteArrayOutputStream.toByteArray();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public SchemaInfo getSchemaInfo() {
            return pulsarSchemaInfo;
        }

        @Override
        public NativeSchemaWrapper clone() {
            return new NativeSchemaWrapper(nativeSchema, pulsarSchemaType);
        }

        @Override
        public void validate(byte[] message) {
            // nothing to do
        }

        @Override
        public boolean supportSchemaVersioning() {
            return true;
        }

        @Override
        public void setSchemaInfoProvider(SchemaInfoProvider schemaInfoProvider) {}

        @Override
        public org.apache.avro.generic.GenericRecord decode(byte[] bytes) {
            throw new UnsupportedOperationException();
        }

        @Override
        public org.apache.avro.generic.GenericRecord decode(byte[] bytes, byte[] schemaVersion) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean requireFetchingSchemaInfo() {
            return true;
        }

        @Override
        public void configureSchemaInfo(
                String topic, String componentName, SchemaInfo schemaInfo) {}

        @Override
        public Optional<Object> getNativeSchema() {
            return Optional.of(nativeSchema);
        }
    }
}
