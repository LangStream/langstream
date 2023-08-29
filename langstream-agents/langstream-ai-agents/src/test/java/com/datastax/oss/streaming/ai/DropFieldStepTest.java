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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.junit.jupiter.api.Test;

public class DropFieldStepTest {

    @Test
    void testAvro() throws Exception {
        RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("record");
        recordSchemaBuilder.field("firstName").type(SchemaType.STRING);
        recordSchemaBuilder.field("lastName").type(SchemaType.STRING);
        recordSchemaBuilder.field("age").type(SchemaType.INT32);

        SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.AVRO);
        GenericSchema<GenericRecord> genericSchema = Schema.generic(schemaInfo);

        GenericRecord genericRecord =
                genericSchema
                        .newRecordBuilder()
                        .set("firstName", "Jane")
                        .set("lastName", "Doe")
                        .set("age", 42)
                        .build();

        Record<GenericObject> record =
                new Utils.TestRecord<>(genericSchema, genericRecord, "test-key");

        DropFieldStep step =
                DropFieldStep.builder().valueFields(Arrays.asList("firstName", "lastName")).build();
        Record<?> outputRecord = Utils.process(record, step);
        assertEquals(outputRecord.getKey().orElse(null), "test-key");

        GenericData.Record read =
                Utils.getRecord(outputRecord.getSchema(), (byte[]) outputRecord.getValue());
        assertEquals(read.get("age"), 42);
        assertNull(read.getSchema().getField("firstName"));
        assertNull(read.getSchema().getField("lastName"));
    }

    @Test
    void testJson() throws Exception {
        RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("record");
        recordSchemaBuilder.field("firstName").type(SchemaType.STRING);
        recordSchemaBuilder.field("lastName").type(SchemaType.STRING);
        recordSchemaBuilder.field("age").type(SchemaType.INT32);

        SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.JSON);
        GenericSchema<GenericRecord> genericSchema = Schema.generic(schemaInfo);

        GenericRecord genericRecord =
                genericSchema
                        .newRecordBuilder()
                        .set("firstName", "Jane")
                        .set("lastName", "Doe")
                        .set("age", 42)
                        .build();

        Record<GenericObject> record =
                new Utils.TestRecord<>(genericSchema, genericRecord, "test-key");

        DropFieldStep step =
                DropFieldStep.builder().valueFields(Arrays.asList("firstName", "lastName")).build();
        Record<?> outputRecord = Utils.process(record, step);
        assertEquals(outputRecord.getKey().orElse(null), "test-key");

        JsonNode read = (JsonNode) outputRecord.getValue();
        assertEquals(read.get("age").asInt(), 42);
        assertNull(read.get("firstName"));
        assertNull(read.get("lastName"));
    }

    @Test
    void testStringJson() throws Exception {
        String value =
                "{\"valueField1\": \"value1\", \"valueField2\": \"value2\", \"valueField3\": \"value3\"}";
        Record<GenericObject> record =
                new Utils.TestRecord<>(
                        Schema.STRING,
                        AutoConsumeSchema.wrapPrimitiveObject(
                                value, SchemaType.STRING, new byte[] {}),
                        "test-key");

        DropFieldStep step =
                DropFieldStep.builder().valueFields(Collections.singletonList("value1")).build();
        Record<?> outputRecord = Utils.process(record, step);
        assertEquals(
                outputRecord.getValue(),
                "{\"valueField1\":\"value1\",\"valueField2\":\"value2\","
                        + "\"valueField3\":\"value3\"}");
    }

    @Test
    void testBytesJson() throws Exception {
        String value =
                "{\"valueField1\": \"value1\", \"valueField2\": \"value2\", \"valueField3\": \"value3\"}";
        Record<GenericObject> record =
                new Utils.TestRecord<>(
                        Schema.BYTES,
                        AutoConsumeSchema.wrapPrimitiveObject(
                                value.getBytes(StandardCharsets.UTF_8),
                                SchemaType.BYTES,
                                new byte[] {}),
                        "test-key");

        DropFieldStep step =
                DropFieldStep.builder()
                        .valueFields(Collections.singletonList("valueField1"))
                        .build();
        Record<?> outputRecord = Utils.process(record, step);
        assertEquals(
                new String((byte[]) outputRecord.getValue(), StandardCharsets.UTF_8),
                "{\"valueField2\":\"value2\",\"valueField3\":\"value3\"}");
    }

    @Test
    void testKeyValueAvro() throws Exception {
        DropFieldStep step =
                DropFieldStep.builder()
                        .keyFields(Arrays.asList("keyField1", "keyField2"))
                        .valueFields(Arrays.asList("valueField1", "valueField2"))
                        .build();
        Record<?> outputRecord = Utils.process(Utils.createTestAvroKeyValueRecord(), step);
        KeyValueSchema<?, ?> messageSchema = (KeyValueSchema<?, ?>) outputRecord.getSchema();
        KeyValue<?, ?> messageValue = (KeyValue<?, ?>) outputRecord.getValue();

        GenericData.Record keyAvroRecord =
                Utils.getRecord(messageSchema.getKeySchema(), (byte[]) messageValue.getKey());
        assertEquals(keyAvroRecord.get("keyField3"), new Utf8("key3"));
        assertNull(keyAvroRecord.getSchema().getField("keyField1"));
        assertNull(keyAvroRecord.getSchema().getField("keyField2"));

        GenericData.Record valueAvroRecord =
                Utils.getRecord(messageSchema.getValueSchema(), (byte[]) messageValue.getValue());
        assertEquals(valueAvroRecord.get("valueField3"), new Utf8("value3"));
        assertNull(valueAvroRecord.getSchema().getField("valueField1"));
        assertNull(valueAvroRecord.getSchema().getField("valueField2"));

        assertEquals(messageSchema.getKeyValueEncodingType(), KeyValueEncodingType.SEPARATED);
    }

    @Test
    void testKeyValueJson() throws Exception {
        DropFieldStep step =
                DropFieldStep.builder()
                        .keyFields(Arrays.asList("keyField1", "keyField2"))
                        .valueFields(Arrays.asList("valueField1", "valueField2"))
                        .build();
        Record<?> outputRecord = Utils.process(Utils.createTestJsonKeyValueRecord(), step);
        KeyValueSchema<?, ?> messageSchema = (KeyValueSchema<?, ?>) outputRecord.getSchema();
        KeyValue<?, ?> messageValue = (KeyValue<?, ?>) outputRecord.getValue();

        JsonNode key = (JsonNode) messageValue.getKey();
        assertEquals(key.get("keyField3").asText(), "key3");
        assertNull(key.get("keyField1"));
        assertNull(key.get("keyField2"));

        JsonNode value = (JsonNode) messageValue.getValue();
        assertEquals(value.get("valueField3").asText(), "value3");
        assertNull(value.get("valueField1"));
        assertNull(value.get("valueField2"));

        assertEquals(messageSchema.getKeyValueEncodingType(), KeyValueEncodingType.SEPARATED);
    }

    @Test
    void testKeyValueStringJson() throws Exception {
        Schema<KeyValue<String, String>> keyValueSchema =
                Schema.KeyValue(Schema.STRING, Schema.STRING, KeyValueEncodingType.SEPARATED);

        String key = "{\"keyField1\": \"key1\", \"keyField2\": \"key2\", \"keyField3\": \"key3\"}";
        String value =
                "{\"valueField1\": \"value1\", \"valueField2\": \"value2\", \"valueField3\": \"value3\"}";

        Record<GenericObject> record =
                new Utils.TestRecord<>(
                        keyValueSchema,
                        AutoConsumeSchema.wrapPrimitiveObject(
                                new KeyValue<>(key, value), SchemaType.KEY_VALUE, new byte[] {}),
                        null);

        DropFieldStep step =
                DropFieldStep.builder()
                        .keyFields(Arrays.asList("keyField1", "keyField2"))
                        .valueFields(Arrays.asList("valueField1", "valueField2"))
                        .build();

        Record<?> outputRecord = Utils.process(record, step);
        KeyValue<?, ?> messageValue = (KeyValue<?, ?>) outputRecord.getValue();

        assertEquals(messageValue.getKey(), "{\"keyField3\":\"key3\"}");
        assertEquals(messageValue.getValue(), "{\"valueField3\":\"value3\"}");
    }

    @Test
    void testKeyValueBytesJson() throws Exception {
        Schema<KeyValue<byte[], byte[]>> keyValueSchema =
                Schema.KeyValue(Schema.BYTES, Schema.BYTES, KeyValueEncodingType.SEPARATED);

        String key = "{\"keyField1\": \"key1\", \"keyField2\": \"key2\", \"keyField3\": \"key3\"}";
        String value =
                "{\"valueField1\": \"value1\", \"valueField2\": \"value2\", \"valueField3\": \"value3\"}";

        Record<GenericObject> record =
                new Utils.TestRecord<>(
                        keyValueSchema,
                        AutoConsumeSchema.wrapPrimitiveObject(
                                new KeyValue<>(
                                        key.getBytes(StandardCharsets.UTF_8),
                                        value.getBytes(StandardCharsets.UTF_8)),
                                SchemaType.KEY_VALUE,
                                new byte[] {}),
                        null);

        DropFieldStep step =
                DropFieldStep.builder()
                        .keyFields(Arrays.asList("keyField1", "keyField2"))
                        .valueFields(Arrays.asList("valueField1", "valueField2"))
                        .build();

        Record<?> outputRecord = Utils.process(record, step);
        KeyValue<?, ?> messageValue = (KeyValue<?, ?>) outputRecord.getValue();

        assertEquals(
                new String((byte[]) messageValue.getKey(), StandardCharsets.UTF_8),
                "{\"keyField3\":\"key3\"}");
        assertEquals(
                new String((byte[]) messageValue.getValue(), StandardCharsets.UTF_8),
                "{\"valueField3\":\"value3\"}");
    }

    @Test
    void testKeyValueAvroCached() throws Exception {
        Record<GenericObject> record = Utils.createTestAvroKeyValueRecord();

        DropFieldStep step =
                DropFieldStep.builder()
                        .keyFields(Arrays.asList("keyField1", "keyField2"))
                        .valueFields(Arrays.asList("valueField1", "valueField2"))
                        .build();
        Record<?> outputRecord = Utils.process(record, step);
        KeyValueSchema<?, ?> messageSchema = (KeyValueSchema<?, ?>) outputRecord.getSchema();

        outputRecord = Utils.process(Utils.createTestAvroKeyValueRecord(), step);
        KeyValueSchema<?, ?> newMessageSchema = (KeyValueSchema<?, ?>) outputRecord.getSchema();

        // Schema was modified by process operation
        KeyValueSchema<?, ?> recordSchema = (KeyValueSchema) record.getSchema();
        assertNotSame(
                messageSchema.getKeySchema().getNativeSchema().orElseThrow(),
                recordSchema.getKeySchema().getNativeSchema().orElseThrow());
        assertNotSame(
                messageSchema.getValueSchema().getNativeSchema().orElseThrow(),
                recordSchema.getValueSchema().getNativeSchema().orElseThrow());

        // Multiple process output the same cached schema
        assertSame(
                messageSchema.getKeySchema().getNativeSchema().orElseThrow(),
                newMessageSchema.getKeySchema().getNativeSchema().orElseThrow());
        assertSame(
                messageSchema.getValueSchema().getNativeSchema().orElseThrow(),
                newMessageSchema.getValueSchema().getNativeSchema().orElseThrow());
    }

    @Test
    void testPrimitives() throws Exception {
        Record<GenericObject> record =
                new Utils.TestRecord<>(
                        Schema.STRING,
                        AutoConsumeSchema.wrapPrimitiveObject(
                                "value", SchemaType.STRING, new byte[] {}),
                        "test-key");

        DropFieldStep step =
                DropFieldStep.builder()
                        .keyFields(Collections.singletonList("key"))
                        .valueFields(Collections.singletonList("value"))
                        .build();
        Record<GenericObject> outputRecord = Utils.process(record, step);

        assertSame(outputRecord.getSchema(), record.getSchema());
        assertSame(outputRecord.getValue(), record.getValue().getNativeObject());
    }

    @Test
    void testKeyValuePrimitives() throws Exception {
        Schema<KeyValue<String, Integer>> keyValueSchema =
                Schema.KeyValue(Schema.STRING, Schema.INT32, KeyValueEncodingType.SEPARATED);

        KeyValue<String, Integer> keyValue = new KeyValue<>("key", 42);

        Record<GenericObject> record =
                new Utils.TestRecord<>(
                        keyValueSchema,
                        AutoConsumeSchema.wrapPrimitiveObject(
                                keyValue, SchemaType.KEY_VALUE, new byte[] {}),
                        null);

        DropFieldStep step =
                DropFieldStep.builder()
                        .keyFields(Collections.singletonList("key"))
                        .valueFields(Collections.singletonList("value"))
                        .build();
        Record<?> outputRecord = Utils.process(record, step);
        KeyValueSchema<?, ?> messageSchema = (KeyValueSchema<?, ?>) outputRecord.getSchema();
        KeyValue<?, ?> messageValue = (KeyValue<?, ?>) outputRecord.getValue();

        KeyValueSchema<?, ?> recordSchema = (KeyValueSchema) record.getSchema();
        KeyValue<?, ?> recordValue = ((KeyValue<?, ?>) record.getValue().getNativeObject());
        assertSame(messageSchema.getKeySchema(), recordSchema.getKeySchema());
        assertSame(messageSchema.getValueSchema(), recordSchema.getValueSchema());
        assertSame(messageValue.getKey(), recordValue.getKey());
        assertSame(messageValue.getValue(), recordValue.getValue());
    }
}
