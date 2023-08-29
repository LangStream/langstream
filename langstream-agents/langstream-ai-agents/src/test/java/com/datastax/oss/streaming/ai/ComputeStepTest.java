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

import static com.datastax.oss.streaming.ai.Utils.assertOptionalField;
import static org.apache.avro.Schema.Type.BOOLEAN;
import static org.apache.avro.Schema.Type.BYTES;
import static org.apache.avro.Schema.Type.DOUBLE;
import static org.apache.avro.Schema.Type.FLOAT;
import static org.apache.avro.Schema.Type.INT;
import static org.apache.avro.Schema.Type.LONG;
import static org.apache.avro.Schema.Type.STRING;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.streaming.ai.model.ComputeField;
import com.datastax.oss.streaming.ai.model.ComputeFieldType;
import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroRecord;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class ComputeStepTest {

    private static final org.apache.avro.Schema STRING_SCHEMA =
            org.apache.avro.Schema.create(STRING);
    private static final org.apache.avro.Schema INT_SCHEMA = org.apache.avro.Schema.create(INT);
    private static final org.apache.avro.Schema LONG_SCHEMA = org.apache.avro.Schema.create(LONG);
    private static final org.apache.avro.Schema FLOAT_SCHEMA = org.apache.avro.Schema.create(FLOAT);
    private static final org.apache.avro.Schema DOUBLE_SCHEMA =
            org.apache.avro.Schema.create(DOUBLE);
    private static final org.apache.avro.Schema BOOLEAN_SCHEMA =
            org.apache.avro.Schema.create(BOOLEAN);
    private static final org.apache.avro.Schema BYTES_SCHEMA = org.apache.avro.Schema.create(BYTES);

    private static final org.apache.avro.Schema DATE_SCHEMA =
            LogicalTypes.date().addToSchema(org.apache.avro.Schema.create(INT));

    private static final org.apache.avro.Schema TIME_SCHEMA =
            LogicalTypes.timeMillis().addToSchema(org.apache.avro.Schema.create(INT));
    private static final org.apache.avro.Schema TIMESTAMP_SCHEMA =
            LogicalTypes.timestampMillis().addToSchema(org.apache.avro.Schema.create(LONG));

    private static final CqlDecimalLogicalType CQL_DECIMAL_LOGICAL_TYPE =
            new CqlDecimalLogicalType();
    private static final String CQL_DECIMAL = "cql_decimal";
    private static final String CQL_DECIMAL_BIGINT = "bigint";
    private static final String CQL_DECIMAL_SCALE = "scale";
    private static final org.apache.avro.Schema decimalType =
            CQL_DECIMAL_LOGICAL_TYPE.addToSchema(
                    org.apache.avro.SchemaBuilder.record(CQL_DECIMAL)
                            .fields()
                            .name(CQL_DECIMAL_BIGINT)
                            .type()
                            .bytesType()
                            .noDefault()
                            .name(CQL_DECIMAL_SCALE)
                            .type()
                            .intType()
                            .noDefault()
                            .endRecord());

    @Test
    void testAvro() throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone(ZoneOffset.UTC));
        RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("record");
        recordSchemaBuilder.field("firstName").type(SchemaType.STRING);
        recordSchemaBuilder.field("lastName").type(SchemaType.STRING);
        recordSchemaBuilder.field("age").type(SchemaType.INT32);
        recordSchemaBuilder.field("date").type(SchemaType.DATE);
        recordSchemaBuilder.field("timestamp").type(SchemaType.TIMESTAMP);
        recordSchemaBuilder.field("time").type(SchemaType.TIME);
        recordSchemaBuilder.field("integerStr").type(SchemaType.STRING);

        SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.AVRO);
        GenericSchema<GenericRecord> genericSchema = Schema.generic(schemaInfo);

        GenericRecord genericRecord =
                genericSchema
                        .newRecordBuilder()
                        .set("firstName", "Jane")
                        .set("lastName", "Doe")
                        .set("age", 42)
                        .set("date", (int) LocalDate.of(2023, 1, 2).toEpochDay())
                        .set("timestamp", Instant.parse("2023-01-02T23:04:05.006Z").toEpochMilli())
                        .set(
                                "time",
                                (int) (LocalTime.parse("23:04:05.006").toNanoOfDay() / 1_000_000))
                        .set("integerStr", "13360")
                        .build();

        Record<GenericObject> record =
                new Utils.TestRecord<>(genericSchema, genericRecord, "test-key");

        List<ComputeField> fields = buildComputeFields("value", false, false, false);
        fields.add(
                ComputeField.builder()
                        .scopedName("value.age")
                        .expression("value.age + 1")
                        .type(ComputeFieldType.STRING)
                        .build());
        fields.add(
                ComputeField.builder()
                        .scopedName("value.dateStr")
                        .expression("value.date")
                        .type(ComputeFieldType.STRING)
                        .build());
        fields.add(
                ComputeField.builder()
                        .scopedName("value.timestampStr")
                        .expression("value.timestamp")
                        .type(ComputeFieldType.STRING)
                        .build());
        fields.add(
                ComputeField.builder()
                        .scopedName("value.timeStr")
                        .expression("value.time")
                        .type(ComputeFieldType.STRING)
                        .build());
        fields.add(
                ComputeField.builder()
                        .scopedName("value.integer")
                        .expression("value.integerStr")
                        .type(ComputeFieldType.INT32)
                        .build());
        ComputeStep step = ComputeStep.builder().fields(fields).build();
        Record<?> outputRecord = Utils.process(record, step);
        assertEquals(outputRecord.getKey().orElse(null), "test-key");

        GenericData.Record read =
                Utils.getRecord(outputRecord.getSchema(), (byte[]) outputRecord.getValue());
        assertEquals(read.get("firstName"), new Utf8("Jane"));
        assertEquals(read.get("dateStr"), new Utf8("2023-01-02"));
        assertEquals(read.get("timestampStr"), new Utf8("2023-01-02T23:04:05.006Z"));
        assertEquals(read.get("timeStr"), new Utf8("23:04:05.006"));
        assertEquals(read.get("integer"), 13360);

        assertTrue(read.hasField("newStringField"));
        assertEquals(read.getSchema().getField("newStringField").schema(), STRING_SCHEMA);
        assertEquals(read.get("newStringField"), new Utf8("Hotaru"));

        assertTrue(read.hasField("newInt8Field"));
        assertEquals(read.getSchema().getField("newInt8Field").schema(), INT_SCHEMA);
        assertEquals(read.get("newInt8Field"), 127);

        assertTrue(read.hasField("newInt16Field"));
        assertEquals(read.getSchema().getField("newInt16Field").schema(), INT_SCHEMA);
        assertEquals(read.get("newInt16Field"), 32767);

        assertTrue(read.hasField("newInt32Field"));
        assertEquals(read.getSchema().getField("newInt32Field").schema(), INT_SCHEMA);
        assertEquals(read.get("newInt32Field"), 2147483647);

        assertTrue(read.hasField("newInt64Field"));
        assertEquals(read.getSchema().getField("newInt64Field").schema(), LONG_SCHEMA);
        assertEquals(read.get("newInt64Field"), 9223372036854775807L);

        assertTrue(read.hasField("newFloatField"));
        assertEquals(read.getSchema().getField("newFloatField").schema(), FLOAT_SCHEMA);
        assertEquals(read.get("newFloatField"), 340282346638528859999999999999999999999.999999F);

        assertTrue(read.hasField("newDoubleField"));
        assertEquals(read.getSchema().getField("newDoubleField").schema(), DOUBLE_SCHEMA);
        assertEquals(read.get("newDoubleField"), 1.79769313486231570e+308D);

        assertTrue(read.hasField("newBooleanField"));
        assertEquals(read.getSchema().getField("newBooleanField").schema(), BOOLEAN_SCHEMA);
        assertTrue((Boolean) read.get("newBooleanField"));

        assertTrue(read.hasField("newBytesField"));
        assertEquals(read.getSchema().getField("newBytesField").schema(), BYTES_SCHEMA);
        assertEquals(
                read.get("newBytesField"),
                ByteBuffer.wrap("Hotaru".getBytes(StandardCharsets.UTF_8)));

        assertTrue(read.hasField("newDateField"));
        assertEquals(read.getSchema().getField("newDateField").schema(), DATE_SCHEMA);
        assertEquals(read.get("newDateField"), 13850); // 13850 days since 1970-01-01

        assertTrue(read.hasField("newDateField2"));
        assertEquals(read.getSchema().getField("newDateField2").schema(), DATE_SCHEMA);
        assertEquals(read.get("newDateField2"), 13850); // 13850 days since 1970-01-01

        assertTrue(read.hasField("newLocalDateField"));
        assertEquals(read.getSchema().getField("newLocalDateField").schema(), DATE_SCHEMA);
        assertEquals(read.get("newLocalDateField"), 13850); // 13850 days since 1970-01-01

        assertTrue(read.hasField("newLocalDateField2"));
        assertEquals(read.getSchema().getField("newLocalDateField2").schema(), DATE_SCHEMA);
        assertEquals(read.get("newLocalDateField2"), 13850); // 13850 days since 1970-01-01

        assertTrue(read.hasField("newTimeField"));
        assertEquals(read.getSchema().getField("newTimeField").schema(), TIME_SCHEMA);
        assertEquals(read.get("newTimeField"), 36930000); // 36930000 ms since 00:00:00

        assertTrue(read.hasField("newTimeField2"));
        assertEquals(read.getSchema().getField("newTimeField2").schema(), TIME_SCHEMA);
        assertEquals(read.get("newTimeField2"), 36930000); // 36930000 ms since 00:00:00

        assertTrue(read.hasField("newLocalTimeField"));
        assertEquals(read.getSchema().getField("newLocalTimeField").schema(), TIME_SCHEMA);
        assertEquals(read.get("newLocalTimeField"), 36930000); // 36930000 ms since 00:00:00

        assertTrue(read.hasField("newLocalTimeField2"));
        assertEquals(read.getSchema().getField("newLocalTimeField2").schema(), TIME_SCHEMA);
        assertEquals(read.get("newLocalTimeField2"), 36930000); // 36930000 ms since 00:00:00

        assertTrue(read.hasField("newDateTimeField"));
        assertEquals(read.getSchema().getField("newDateTimeField").schema(), TIMESTAMP_SCHEMA);
        assertEquals(read.get("newDateTimeField"), 1196676930000L);

        assertTrue(read.hasField("newInstantField"));
        assertEquals(read.getSchema().getField("newInstantField").schema(), TIMESTAMP_SCHEMA);
        assertEquals(read.get("newInstantField"), 1196676930000L);

        assertTrue(read.hasField("newInstantField2"));
        assertEquals(read.getSchema().getField("newInstantField2").schema(), TIMESTAMP_SCHEMA);
        assertEquals(read.get("newInstantField2"), 1196676930000L);

        assertTrue(read.hasField("newTimestampField"));
        assertEquals(read.getSchema().getField("newTimestampField").schema(), TIMESTAMP_SCHEMA);
        assertEquals(read.get("newTimestampField"), 1196676930000L);

        assertTrue(read.hasField("newTimestampField2"));
        assertEquals(read.getSchema().getField("newTimestampField2").schema(), TIMESTAMP_SCHEMA);
        assertEquals(read.get("newTimestampField2"), 1196676930000L);

        assertTrue(read.hasField("newLocalDateTimeField"));
        assertEquals(read.getSchema().getField("newLocalDateTimeField").schema(), TIMESTAMP_SCHEMA);
        assertEquals(read.get("newLocalDateTimeField"), 1196676930000L);

        assertTrue(read.hasField("newLocalDateTimeField2"));
        assertEquals(
                read.getSchema().getField("newLocalDateTimeField2").schema(), TIMESTAMP_SCHEMA);
        assertEquals(read.get("newLocalDateTimeField2"), 1196676930000L);

        assertEquals(read.getSchema().getField("age").schema(), STRING_SCHEMA);
        assertEquals(read.get("age"), new Utf8("43"));
    }

    @Test
    void testJson() throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone(ZoneOffset.UTC));
        RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("record");
        recordSchemaBuilder.field("firstName").type(SchemaType.STRING);
        recordSchemaBuilder.field("lastName").type(SchemaType.STRING);
        recordSchemaBuilder.field("age").type(SchemaType.INT32);
        recordSchemaBuilder.field("date").type(SchemaType.DATE);
        recordSchemaBuilder.field("timestamp").type(SchemaType.TIMESTAMP);
        recordSchemaBuilder.field("time").type(SchemaType.TIME);
        recordSchemaBuilder.field("integerStr").type(SchemaType.STRING);

        SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.JSON);
        GenericSchema<GenericRecord> genericSchema = Schema.generic(schemaInfo);

        GenericRecord genericRecord =
                genericSchema
                        .newRecordBuilder()
                        .set("firstName", "Jane")
                        .set("lastName", "Doe")
                        .set("age", 42)
                        .set("date", (int) LocalDate.of(2023, 1, 2).toEpochDay())
                        .set("timestamp", Instant.parse("2023-01-02T23:04:05.006Z").toEpochMilli())
                        .set(
                                "time",
                                (int) (LocalTime.parse("23:04:05.006").toNanoOfDay() / 1_000_000))
                        .set("integerStr", "13360")
                        .build();

        Record<GenericObject> record =
                new Utils.TestRecord<>(genericSchema, genericRecord, "test-key");

        List<ComputeField> fields = buildComputeFields("value", false, false, false);
        fields.add(
                ComputeField.builder()
                        .scopedName("value.age")
                        .expression("value.age + 1")
                        .type(ComputeFieldType.STRING)
                        .build());
        fields.add(
                ComputeField.builder()
                        .scopedName("value.dateStr")
                        .expression("value.date")
                        .type(ComputeFieldType.STRING)
                        .build());
        fields.add(
                ComputeField.builder()
                        .scopedName("value.timestampStr")
                        .expression("value.timestamp")
                        .type(ComputeFieldType.STRING)
                        .build());
        fields.add(
                ComputeField.builder()
                        .scopedName("value.timeStr")
                        .expression("value.time")
                        .type(ComputeFieldType.STRING)
                        .build());
        fields.add(
                ComputeField.builder()
                        .scopedName("value.integer")
                        .expression("value.integerStr")
                        .type(ComputeFieldType.INT32)
                        .build());
        ComputeStep step = ComputeStep.builder().fields(fields).build();
        Record<?> outputRecord = Utils.process(record, step);
        assertEquals(outputRecord.getKey().orElse(null), "test-key");
        JsonNode read = (JsonNode) outputRecord.getValue();
        assertEquals(read.get("firstName").asText(), "Jane");
        assertEquals(read.get("dateStr").asText(), "2023-01-02");
        assertEquals(read.get("timestampStr").asText(), "2023-01-02T23:04:05.006Z");
        assertEquals(read.get("timeStr").asText(), "23:04:05.006");
        assertEquals(read.get("integer").asInt(), 13360);
        assertEquals(read.get("newStringField").asText(), "Hotaru");
        assertEquals(read.get("newInt8Field").asInt(), 127);
        assertEquals(read.get("newInt16Field").asInt(), 32767);
        assertEquals(read.get("newInt32Field").asInt(), 2147483647);
        assertEquals(read.get("newInt64Field").asLong(), 9223372036854775807L);
        assertEquals(
                read.get("newFloatField").asDouble(),
                340282346638528859999999999999999999999.999999F);
        assertEquals(read.get("newDoubleField").asDouble(), 1.79769313486231570e+308D);
        assertTrue(read.get("newBooleanField").asBoolean());
        assertEquals(
                read.get("newBytesField").asText(),
                Base64.getEncoder().encodeToString("Hotaru".getBytes(StandardCharsets.UTF_8)));
        assertEquals(read.get("newDateField").asInt(), 13850); // 13850 days since 1970-01-01
        assertEquals(read.get("newDateField2").asInt(), 13850); // 13850 days since 1970-01-01
        assertEquals(read.get("newLocalDateField").asInt(), 13850); // 13850 days since 1970-01-01
        assertEquals(read.get("newLocalDateField2").asInt(), 13850); // 13850 days since 1970-01-01
        assertEquals(read.get("newTimeField").asInt(), 36930000); // 36930000 ms since 00:00:00
        assertEquals(read.get("newTimeField2").asInt(), 36930000); // 36930000 ms since 00:00:00
        assertEquals(read.get("newLocalTimeField").asInt(), 36930000); // 36930000 ms since 00:00:00
        assertEquals(
                read.get("newLocalTimeField2").asInt(), 36930000); // 36930000 ms since 00:00:00
        assertEquals(read.get("newDateTimeField").asLong(), 1196676930000L);
        assertEquals(read.get("newInstantField").asLong(), 1196676930000L);
        assertEquals(read.get("newInstantField2").asLong(), 1196676930000L);
        assertEquals(read.get("newTimestampField").asLong(), 1196676930000L);
        assertEquals(read.get("newTimestampField2").asLong(), 1196676930000L);
        assertEquals(read.get("newLocalDateTimeField").asLong(), 1196676930000L);
        assertEquals(read.get("newLocalDateTimeField2").asLong(), 1196676930000L);
        assertEquals(read.get("age").asText(), "43");
    }

    public static Object[][] jsonStringSchemas() {
        return new Object[][] {{Schema.STRING}, {Schema.BYTES}};
    }

    @ParameterizedTest
    @MethodSource("jsonStringSchemas")
    void testStringJson(Schema schema) throws Exception {
        SchemaType schemaType = schema.getSchemaInfo().getType();
        Object json =
                "{\"name\":\"Jane\",\"age\":42,\"date\":18999,\"timestamp\":1672525445006,\"time\":83085006,\"integerStr\":\"13360\"}";
        if (schemaType == SchemaType.BYTES) {
            json = ((String) json).getBytes(StandardCharsets.UTF_8);
        }
        Record<GenericObject> record =
                new Utils.TestRecord<>(
                        schema,
                        AutoConsumeSchema.wrapPrimitiveObject(json, schemaType, new byte[] {}),
                        "test-key");

        List<ComputeField> fields =
                Collections.singletonList(
                        ComputeField.builder()
                                .scopedName("value.age")
                                .expression("value.age + 1")
                                .type(ComputeFieldType.STRING)
                                .build());
        ComputeStep step = ComputeStep.builder().fields(fields).build();
        Record<?> outputRecord = Utils.process(record, step);

        assertEquals(outputRecord.getSchema(), schema);

        Object expected =
                "{\"name\":\"Jane\",\"age\":\"43\",\"date\":18999,\"timestamp\":1672525445006,\"time\":83085006,"
                        + "\"integerStr\":\"13360\"}";
        if (schemaType == SchemaType.BYTES) {
            expected = ((String) expected).getBytes(StandardCharsets.UTF_8);
            assertArrayEquals((byte[]) outputRecord.getValue(), (byte[]) expected);
        } else {
            assertEquals(outputRecord.getValue(), expected);
        }
    }

    @ParameterizedTest
    @MethodSource("jsonStringSchemas")
    void testKVStringJson(Schema<?> schema) throws Exception {
        SchemaType schemaType = schema.getSchemaInfo().getType();
        Object json =
                "{\"name\":\"Jane\",\"age\":42,\"date\":18999,\"timestamp\":1672525445006,\"time\":83085006,\"integerStr\":\"13360\"}";
        if (schemaType == SchemaType.BYTES) {
            json = ((String) json).getBytes(StandardCharsets.UTF_8);
        }

        Schema<? extends KeyValue<?, Integer>> keyValueSchema =
                Schema.KeyValue(schema, Schema.INT32, KeyValueEncodingType.SEPARATED);

        KeyValue<Object, Integer> keyValue = new KeyValue<>(json, 42);

        Record<GenericObject> record =
                new Utils.TestRecord<>(
                        keyValueSchema,
                        AutoConsumeSchema.wrapPrimitiveObject(
                                keyValue, SchemaType.KEY_VALUE, new byte[] {}),
                        "test-key");

        List<ComputeField> fields =
                List.of(
                        ComputeField.builder()
                                .scopedName("value.age")
                                .expression("value.age + 1")
                                .type(ComputeFieldType.STRING)
                                .build(),
                        ComputeField.builder()
                                .scopedName("key.age")
                                .expression("key.age + 2")
                                .type(ComputeFieldType.STRING)
                                .build());
        ComputeStep step = ComputeStep.builder().fields(fields).build();

        Record<?> outputRecord = Utils.process(record, step);
        KeyValueSchema<?, ?> messageSchema = (KeyValueSchema<?, ?>) outputRecord.getSchema();
        KeyValue<?, ?> messageValue = (KeyValue<?, ?>) outputRecord.getValue();

        assertEquals(messageSchema.getKeySchema(), schema);
        assertEquals(messageSchema.getValueSchema(), Schema.INT32);

        assertEquals(messageValue.getValue(), 42);
        Object expected =
                "{\"name\":\"Jane\",\"age\":\"44\",\"date\":18999,\"timestamp\":1672525445006,\"time\":83085006,"
                        + "\"integerStr\":\"13360\"}";
        if (schemaType == SchemaType.BYTES) {
            expected = ((String) expected).getBytes(StandardCharsets.UTF_8);
            assertArrayEquals((byte[]) messageValue.getKey(), (byte[]) expected);
        } else {
            assertEquals(messageValue.getKey(), expected);
        }
    }

    @Test
    void testAvroInferredType() throws Exception {
        RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("record");
        recordSchemaBuilder.field("firstName").type(SchemaType.STRING);
        recordSchemaBuilder.field("lastName").type(SchemaType.STRING);
        recordSchemaBuilder.field("age").type(SchemaType.INT32);
        recordSchemaBuilder.field("size").type(SchemaType.FLOAT);
        recordSchemaBuilder.field("date").type(SchemaType.DATE);
        recordSchemaBuilder.field("timestamp").type(SchemaType.TIMESTAMP);
        recordSchemaBuilder.field("time").type(SchemaType.TIME);

        SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.AVRO);
        GenericSchema<GenericRecord> genericSchema = Schema.generic(schemaInfo);

        int date = (int) LocalDate.of(2023, 1, 2).toEpochDay();
        long timestampMillis = Instant.parse("2023-01-02T23:04:05.006Z").toEpochMilli();
        int timeMillis = (int) (LocalTime.parse("23:04:05.006").toNanoOfDay() / 1_000_000);
        GenericRecord genericRecord =
                genericSchema
                        .newRecordBuilder()
                        .set("firstName", "Jane")
                        .set("lastName", "Doe")
                        .set("age", 42)
                        .set("size", 5.6F)
                        .set("date", date)
                        .set("timestamp", timestampMillis)
                        .set("time", timeMillis)
                        .build();

        Record<GenericObject> record =
                new Utils.TestRecord<>(genericSchema, genericRecord, "test-key");

        List<ComputeField> fields = buildComputeFields("value", false, false, true);
        fields.add(
                ComputeField.builder()
                        .scopedName("value.newSizeField")
                        .expression("value.size")
                        .build());
        fields.add(
                ComputeField.builder()
                        .scopedName("value.newDate")
                        .expression("value.date")
                        .build());
        fields.add(
                ComputeField.builder()
                        .scopedName("value.newTimestamp")
                        .expression("value.timestamp")
                        .build());
        fields.add(
                ComputeField.builder()
                        .scopedName("value.newTime")
                        .expression("value.time")
                        .build());
        ComputeStep step = ComputeStep.builder().fields(fields).build();
        Record<?> outputRecord = Utils.process(record, step);
        assertEquals(outputRecord.getKey().orElse(null), "test-key");

        GenericData.Record read =
                Utils.getRecord(outputRecord.getSchema(), (byte[]) outputRecord.getValue());
        org.apache.avro.Schema avroSchema =
                (org.apache.avro.Schema) outputRecord.getSchema().getNativeSchema().orElseThrow();

        assertEquals(read.get("firstName"), new Utf8("Jane"));
        assertEquals(read.get("newSizeField"), 5.6F);
        assertEquals(avroSchema.getField("newDate").schema().getLogicalType(), LogicalTypes.date());
        assertEquals(read.get("newDate"), date);
        assertEquals(
                avroSchema.getField("newTimestamp").schema().getLogicalType(),
                LogicalTypes.timestampMillis());
        assertEquals(read.get("newTimestamp"), timestampMillis);
        assertEquals(
                avroSchema.getField("newTime").schema().getLogicalType(),
                LogicalTypes.timeMillis());
        assertEquals(read.get("newTime"), timeMillis);

        assertTrue(read.hasField("newStringField"));
        assertEquals(read.getSchema().getField("newStringField").schema(), STRING_SCHEMA);
        assertEquals(read.get("newStringField"), new Utf8("Hotaru"));

        assertTrue(read.hasField("newInt8Field"));
        assertEquals(read.getSchema().getField("newInt8Field").schema(), LONG_SCHEMA);
        assertEquals(read.get("newInt8Field"), 127L);

        assertTrue(read.hasField("newInt16Field"));
        assertEquals(read.getSchema().getField("newInt16Field").schema(), LONG_SCHEMA);
        assertEquals(read.get("newInt16Field"), 32767L);

        assertTrue(read.hasField("newInt32Field"));
        assertEquals(read.getSchema().getField("newInt32Field").schema(), LONG_SCHEMA);
        assertEquals(read.get("newInt32Field"), 2147483647L);

        assertTrue(read.hasField("newInt64Field"));
        assertEquals(read.getSchema().getField("newInt64Field").schema(), LONG_SCHEMA);
        assertEquals(read.get("newInt64Field"), 9223372036854775807L);

        assertTrue(read.hasField("newFloatField"));
        assertEquals(read.getSchema().getField("newFloatField").schema(), DOUBLE_SCHEMA);
        assertEquals(read.get("newFloatField"), 340282346638528859999999999999999999999.999999D);

        assertTrue(read.hasField("newDoubleField"));
        assertEquals(read.getSchema().getField("newDoubleField").schema(), DOUBLE_SCHEMA);
        assertEquals(read.get("newDoubleField"), 1.79769313486231570e+308D);

        assertTrue(read.hasField("newBooleanField"));
        assertEquals(read.getSchema().getField("newBooleanField").schema(), BOOLEAN_SCHEMA);
        assertTrue((Boolean) read.get("newBooleanField"));

        assertTrue(read.hasField("newBytesField"));
        assertEquals(read.getSchema().getField("newBytesField").schema(), BYTES_SCHEMA);
        assertEquals(
                read.get("newBytesField"),
                ByteBuffer.wrap("Hotaru".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void testAvroNullsNotAllowed() throws Exception {
        assertThrows(
                AvroRuntimeException.class,
                () -> {
                    RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("record");
                    recordSchemaBuilder.field("firstName").type(SchemaType.STRING);

                    SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.AVRO);
                    GenericSchema<GenericRecord> genericSchema = Schema.generic(schemaInfo);

                    GenericRecord genericRecord =
                            genericSchema.newRecordBuilder().set("firstName", "Jane").build();

                    Record<GenericObject> record =
                            new Utils.TestRecord<>(genericSchema, genericRecord, "test-key");

                    ComputeStep step =
                            ComputeStep.builder()
                                    .fields(
                                            Collections.singletonList(
                                                    ComputeField.builder()
                                                            .scopedName("value.newLongField")
                                                            .expression("null")
                                                            .optional(false)
                                                            .type(ComputeFieldType.INT64)
                                                            .build()))
                                    .build();
                    Utils.process(record, step);
                });
    }

    @Test
    void testAvroNullInferredType() throws Exception {
        assertThrows(
                UnsupportedOperationException.class,
                () -> {
                    RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("record");

                    SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.AVRO);
                    GenericSchema<GenericRecord> genericSchema = Schema.generic(schemaInfo);

                    GenericRecord genericRecord = genericSchema.newRecordBuilder().build();

                    Record<GenericObject> record =
                            new Utils.TestRecord<>(genericSchema, genericRecord, "test-key");

                    List<ComputeField> fields = new ArrayList<>();
                    fields.add(
                            ComputeField.builder()
                                    .scopedName("value.newStringField")
                                    .expression("null")
                                    .build());
                    ComputeStep step = ComputeStep.builder().fields(fields).build();
                    Utils.process(record, step);
                });
    }

    @Test
    void testAvroWithNonNullOptionalFields() throws Exception {
        RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("record");
        recordSchemaBuilder.field("firstName").type(SchemaType.STRING);

        SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.AVRO);
        GenericSchema<GenericRecord> genericSchema = Schema.generic(schemaInfo);

        GenericRecord genericRecord =
                genericSchema.newRecordBuilder().set("firstName", "Jane").build();

        Record<GenericObject> record =
                new Utils.TestRecord<>(genericSchema, genericRecord, "test-key");

        ComputeStep step =
                ComputeStep.builder()
                        .fields(buildComputeFields("value", true, false, false))
                        .build();
        Record<?> outputRecord = Utils.process(record, step);
        assertEquals(outputRecord.getKey().orElse(null), "test-key");

        GenericData.Record read =
                Utils.getRecord(outputRecord.getSchema(), (byte[]) outputRecord.getValue());
        assertEquals(read.get("firstName"), new Utf8("Jane"));
        assertOptionalField(read, "newStringField", STRING, new Utf8("Hotaru"));
        assertOptionalField(read, "newInt32Field", INT, 2147483647);
        assertOptionalField(read, "newInt64Field", LONG, 9223372036854775807L);
        assertOptionalField(
                read, "newFloatField", FLOAT, 340282346638528859999999999999999999999.999999F);
        assertOptionalField(read, "newDoubleField", DOUBLE, 1.79769313486231570e+308D);
        assertOptionalField(read, "newBooleanField", BOOLEAN, true);
        assertOptionalField(
                read,
                "newBytesField",
                BYTES,
                ByteBuffer.wrap("Hotaru".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void testAvroWithNullOptionalFields() throws Exception {
        RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("record");
        recordSchemaBuilder.field("firstName").type(SchemaType.STRING);

        SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.AVRO);
        GenericSchema<GenericRecord> genericSchema = Schema.generic(schemaInfo);

        GenericRecord genericRecord =
                genericSchema.newRecordBuilder().set("firstName", "Jane").build();

        Record<GenericObject> record =
                new Utils.TestRecord<>(genericSchema, genericRecord, "test-key");

        ComputeStep step =
                ComputeStep.builder()
                        .fields(buildComputeFields("value", true, true, false))
                        .build();
        Record<?> outputRecord = Utils.process(record, step);
        assertEquals(outputRecord.getKey().orElse(null), "test-key");

        GenericData.Record read =
                Utils.getRecord(outputRecord.getSchema(), (byte[]) outputRecord.getValue());
        assertEquals(read.get("firstName"), new Utf8("Jane"));
        assertOptionalFieldNull(read, "newStringField", STRING);
        assertOptionalFieldNull(read, "newInt32Field", INT);
        assertOptionalFieldNull(read, "newInt64Field", LONG);
        assertOptionalFieldNull(read, "newFloatField", FLOAT);
        assertOptionalFieldNull(read, "newDoubleField", DOUBLE);
        assertOptionalFieldNull(read, "newBooleanField", BOOLEAN);
        assertOptionalFieldNull(read, "newBytesField", BYTES);
    }

    @Test
    void testAvroWithCqlDecimal() throws Exception {
        List<org.apache.avro.Schema.Field> fields = new ArrayList<>();
        fields.add(createDecimalField("cqlDecimalField"));

        org.apache.avro.Schema avroSchema =
                org.apache.avro.Schema.createRecord("custom", "", "ns1", false, fields);
        org.apache.avro.generic.GenericRecord avroRecord = new GenericData.Record(avroSchema);
        BigDecimal decimal =
                new BigDecimal(new BigInteger("1234567890123456789012345678901234567890"), 38);
        avroRecord.put("cqlDecimalField", createDecimalRecord(decimal));
        Schema<org.apache.avro.generic.GenericRecord> genericSchema =
                new Utils.NativeSchemaWrapper(avroSchema, SchemaType.AVRO);
        List<Field> pulsarFields =
                fields.stream().map(v -> new Field(v.name(), v.pos())).collect(Collectors.toList());
        GenericAvroRecord genericRecord =
                new GenericAvroRecord(new byte[0], avroSchema, pulsarFields, avroRecord);
        Record<GenericObject> record =
                new Utils.TestRecord<>(genericSchema, genericRecord, "test-key");

        ComputeStep step =
                ComputeStep.builder()
                        .fields(
                                List.of(
                                        ComputeField.builder()
                                                .scopedName("value.decimalField")
                                                .expression(
                                                        "fn:decimalFromUnscaled(value.cqlDecimalField.bigint, value.cqlDecimalField.scale)")
                                                .type(ComputeFieldType.DECIMAL)
                                                .build(),
                                        ComputeField.builder()
                                                .scopedName("value.decimalFieldFromDouble")
                                                .expression("fn:decimalFromNumber(12.23)")
                                                .type(ComputeFieldType.DECIMAL)
                                                .build()))
                        .build();
        Record<?> outputRecord = Utils.process(record, step);

        GenericData.Record read =
                Utils.getRecord(outputRecord.getSchema(), (byte[]) outputRecord.getValue());
        assertEquals(read.get("decimalField"), decimal);
        assertEquals(read.get("decimalFieldFromDouble"), BigDecimal.valueOf(12.23d));
    }

    @Test
    void testKeyValueAvro() throws Exception {
        ComputeStep step =
                ComputeStep.builder()
                        .fields(
                                Arrays.asList(
                                        ComputeField.builder()
                                                .scopedName("value.newValueStringField")
                                                .expression("value.valueField1")
                                                .type(ComputeFieldType.STRING)
                                                .build(),
                                        ComputeField.builder()
                                                .scopedName("key.newKeyStringField")
                                                .expression("key.keyField1")
                                                .type(ComputeFieldType.STRING)
                                                .build()))
                        .build();

        Record<?> outputRecord = Utils.process(Utils.createTestAvroKeyValueRecord(), step);
        KeyValueSchema<?, ?> messageSchema = (KeyValueSchema<?, ?>) outputRecord.getSchema();
        KeyValue<?, ?> messageValue = (KeyValue<?, ?>) outputRecord.getValue();

        GenericData.Record keyAvroRecord =
                Utils.getRecord(messageSchema.getKeySchema(), (byte[]) messageValue.getKey());
        assertEquals(keyAvroRecord.getSchema().getFields().size(), 4);
        assertEquals(keyAvroRecord.get("keyField1"), new Utf8("key1"));
        assertEquals(keyAvroRecord.get("keyField2"), new Utf8("key2"));
        assertEquals(keyAvroRecord.get("keyField3"), new Utf8("key3"));

        assertTrue(keyAvroRecord.hasField("newKeyStringField"));
        assertEquals(
                keyAvroRecord.getSchema().getField("newKeyStringField").schema().getType(), STRING);
        assertEquals(keyAvroRecord.get("newKeyStringField"), new Utf8("key1"));

        GenericData.Record valueAvroRecord =
                Utils.getRecord(messageSchema.getValueSchema(), (byte[]) messageValue.getValue());
        assertEquals(valueAvroRecord.getSchema().getFields().size(), 4);
        assertEquals(valueAvroRecord.get("valueField1"), new Utf8("value1"));
        assertEquals(valueAvroRecord.get("valueField2"), new Utf8("value2"));
        assertEquals(valueAvroRecord.get("valueField3"), new Utf8("value3"));

        assertTrue(valueAvroRecord.hasField("newValueStringField"));
        assertEquals(
                valueAvroRecord.getSchema().getField("newValueStringField").schema().getType(),
                STRING);
        assertEquals(valueAvroRecord.get("newValueStringField"), new Utf8("value1"));

        assertEquals(messageSchema.getKeyValueEncodingType(), KeyValueEncodingType.SEPARATED);
    }

    @Test
    void testKeyValueJson() throws Exception {
        ComputeStep step =
                ComputeStep.builder()
                        .fields(
                                Arrays.asList(
                                        ComputeField.builder()
                                                .scopedName("value.newValueStringField")
                                                .expression("value.valueField1")
                                                .type(ComputeFieldType.STRING)
                                                .build(),
                                        ComputeField.builder()
                                                .scopedName("key.newKeyStringField")
                                                .expression("key.keyField1")
                                                .type(ComputeFieldType.STRING)
                                                .build()))
                        .build();

        Record<?> outputRecord = Utils.process(Utils.createTestJsonKeyValueRecord(), step);
        KeyValueSchema<?, ?> messageSchema = (KeyValueSchema<?, ?>) outputRecord.getSchema();
        KeyValue<?, ?> messageValue = (KeyValue<?, ?>) outputRecord.getValue();

        JsonNode key = (JsonNode) messageValue.getKey();
        assertEquals(key.get("keyField1").asText(), "key1");
        assertEquals(key.get("keyField2").asText(), "key2");
        assertEquals(key.get("keyField3").asText(), "key3");
        assertEquals(key.get("newKeyStringField").asText(), "key1");

        JsonNode value = (JsonNode) messageValue.getValue();
        assertEquals(value.get("valueField1").asText(), "value1");
        assertEquals(value.get("valueField2").asText(), "value2");
        assertEquals(value.get("valueField3").asText(), "value3");
        assertEquals(value.get("newValueStringField").asText(), "value1");

        assertEquals(messageSchema.getKeyValueEncodingType(), KeyValueEncodingType.SEPARATED);
    }

    @ParameterizedTest
    @MethodSource("primitiveSchemaTypesProvider")
    void testPrimitiveSchemaTypes(
            String expression,
            ComputeFieldType newSchema,
            Object expectedValue,
            Schema<?> expectedSchema)
            throws Exception {
        Record<GenericObject> record =
                new Utils.TestRecord<>(
                        Schema.STRING,
                        AutoConsumeSchema.wrapPrimitiveObject("", SchemaType.STRING, new byte[] {}),
                        "test-key");

        ComputeStep step =
                ComputeStep.builder()
                        .fields(
                                Collections.singletonList(
                                        ComputeField.builder()
                                                .scopedName("value")
                                                .expression(expression)
                                                .type(newSchema)
                                                .build()))
                        .build();

        Record<?> outputRecord = Utils.process(record, step);

        assertEquals(outputRecord.getSchema(), expectedSchema);
        if (expectedValue instanceof byte[]) {
            assertArrayEquals((byte[]) outputRecord.getValue(), (byte[]) expectedValue);
        } else {
            assertEquals(outputRecord.getValue(), expectedValue);
        }
    }

    @ParameterizedTest
    @MethodSource("primitiveInferredSchemaTypesProvider")
    void testPrimitiveInferredSchemaTypes(
            Object input,
            Schema<?> inputSchema,
            String expression,
            Object expectedValue,
            Schema<?> expectedSchema)
            throws Exception {
        Record<GenericObject> record =
                new Utils.TestRecord<>(
                        inputSchema,
                        AutoConsumeSchema.wrapPrimitiveObject(
                                input, inputSchema.getSchemaInfo().getType(), new byte[] {}),
                        "");

        ComputeStep step =
                ComputeStep.builder()
                        .fields(
                                Collections.singletonList(
                                        ComputeField.builder()
                                                .scopedName("value")
                                                .expression(expression)
                                                .build()))
                        .build();

        Record<?> outputRecord = Utils.process(record, step);

        assertEquals(outputRecord.getSchema(), expectedSchema);
        if (expectedValue instanceof byte[]) {
            assertArrayEquals((byte[]) outputRecord.getValue(), (byte[]) expectedValue);
        } else {
            assertEquals(outputRecord.getValue(), expectedValue);
        }
    }

    @Test
    void testNullInferredSchemaType() throws Exception {
        assertThrows(
                UnsupportedOperationException.class,
                () -> {
                    Record<GenericObject> record =
                            new Utils.TestRecord<>(
                                    Schema.STRING,
                                    AutoConsumeSchema.wrapPrimitiveObject(
                                            "input", SchemaType.STRING, new byte[] {}),
                                    "");

                    ComputeStep step =
                            ComputeStep.builder()
                                    .fields(
                                            Collections.singletonList(
                                                    ComputeField.builder()
                                                            .scopedName("value")
                                                            .expression("null")
                                                            .build()))
                                    .build();

                    Utils.process(record, step);
                });
    }

    @Test
    void testKeyValuePrimitivesModified() throws Exception {
        Schema<KeyValue<String, Integer>> keyValueSchema =
                Schema.KeyValue(Schema.STRING, Schema.INT32, KeyValueEncodingType.SEPARATED);

        KeyValue<String, Integer> keyValue = new KeyValue<>("key", 42);

        Record<GenericObject> record =
                new Utils.TestRecord<>(
                        keyValueSchema,
                        AutoConsumeSchema.wrapPrimitiveObject(
                                keyValue, SchemaType.KEY_VALUE, new byte[] {}),
                        null);

        ComputeStep step =
                ComputeStep.builder()
                        .fields(
                                Arrays.asList(
                                        ComputeField.builder()
                                                .scopedName("key")
                                                .expression("88")
                                                .type(ComputeFieldType.INT32)
                                                .build(),
                                        ComputeField.builder()
                                                .scopedName("value")
                                                .expression("'newValue'")
                                                .type(ComputeFieldType.STRING)
                                                .build()))
                        .build();

        Record<?> outputRecord = Utils.process(record, step);
        KeyValueSchema<?, ?> messageSchema = (KeyValueSchema<?, ?>) outputRecord.getSchema();
        KeyValue<?, ?> messageValue = (KeyValue<?, ?>) outputRecord.getValue();

        assertEquals(messageSchema.getKeySchema(), Schema.INT32);
        assertEquals(messageSchema.getValueSchema(), Schema.STRING);

        assertEquals(messageValue.getKey(), 88);
        assertEquals(messageValue.getValue(), "newValue");
    }

    @Test
    void testPrimitivesNotModified() throws Exception {
        Record<GenericObject> record =
                new Utils.TestRecord<>(
                        Schema.STRING,
                        AutoConsumeSchema.wrapPrimitiveObject(
                                "value", SchemaType.STRING, new byte[] {}),
                        "test-key");

        ComputeStep step =
                ComputeStep.builder()
                        .fields(
                                Collections.singletonList(
                                        ComputeField.builder()
                                                .scopedName("value.newField")
                                                .expression("newValue")
                                                .type(ComputeFieldType.STRING)
                                                .build()))
                        .build();

        Record<GenericObject> outputRecord = Utils.process(record, step);

        assertSame(outputRecord.getSchema(), record.getSchema());
        assertSame(outputRecord.getValue(), record.getValue().getNativeObject());
    }

    @Test
    void testKeyValuePrimitivesNotModified() throws Exception {
        Schema<KeyValue<String, Integer>> keyValueSchema =
                Schema.KeyValue(Schema.STRING, Schema.INT32, KeyValueEncodingType.SEPARATED);

        KeyValue<String, Integer> keyValue = new KeyValue<>("key", 42);

        Record<GenericObject> record =
                new Utils.TestRecord<>(
                        keyValueSchema,
                        AutoConsumeSchema.wrapPrimitiveObject(
                                keyValue, SchemaType.KEY_VALUE, new byte[] {}),
                        null);

        ComputeStep step =
                ComputeStep.builder()
                        .fields(
                                Arrays.asList(
                                        ComputeField.builder()
                                                .scopedName("value.newField")
                                                .expression("newValue")
                                                .type(ComputeFieldType.STRING)
                                                .build(),
                                        ComputeField.builder()
                                                .scopedName("key.newField")
                                                .expression("newValue")
                                                .type(ComputeFieldType.STRING)
                                                .build()))
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

    @ParameterizedTest
    @MethodSource("destinationTopicProvider")
    void testAvroComputeDestinationTopic(String topic) throws Exception {
        RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("record");
        recordSchemaBuilder.field("firstName").type(SchemaType.STRING);

        SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.AVRO);
        GenericSchema<GenericRecord> genericSchema = Schema.generic(schemaInfo);

        GenericRecord genericRecord =
                genericSchema.newRecordBuilder().set("firstName", "Jane").build();

        Record<GenericObject> record =
                Utils.TestRecord.<GenericObject>builder()
                        .schema(genericSchema)
                        .value(genericRecord)
                        .destinationTopic(topic)
                        .build();

        List<ComputeField> fields = buildComputeFields("value", false, false, false);
        fields.add(
                ComputeField.builder()
                        .scopedName("destinationTopic")
                        .expression("destinationTopic == 'targetTopic' ? 'route' : 'dont-route'")
                        .type(ComputeFieldType.STRING)
                        .build());
        ComputeStep step = ComputeStep.builder().fields(fields).build();
        Record<?> outputRecord = Utils.process(record, step);
        GenericData.Record read =
                Utils.getRecord(outputRecord.getSchema(), (byte[]) outputRecord.getValue());

        assertEquals(
                outputRecord.getDestinationTopic().orElseThrow(),
                topic.equals("targetTopic") ? "route" : "dont-route");
        assertEquals(read.get("firstName"), new Utf8("Jane"));
    }

    @Test
    void testAvroComputeMessageKey() throws Exception {
        RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("record");
        recordSchemaBuilder.field("firstName").type(SchemaType.STRING);

        SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.AVRO);
        GenericSchema<GenericRecord> genericSchema = Schema.generic(schemaInfo);

        GenericRecord genericRecord =
                genericSchema.newRecordBuilder().set("firstName", "Jane").build();

        Record<GenericObject> record =
                Utils.TestRecord.<GenericObject>builder()
                        .schema(genericSchema)
                        .value(genericRecord)
                        .key("old")
                        .build();

        List<ComputeField> fields = buildComputeFields("value", false, false, false);
        fields.add(
                ComputeField.builder()
                        .scopedName("messageKey")
                        .expression("'new'")
                        .type(ComputeFieldType.STRING)
                        .build());
        ComputeStep step = ComputeStep.builder().fields(fields).build();
        Record<?> outputRecord = Utils.process(record, step);
        GenericData.Record read =
                Utils.getRecord(outputRecord.getSchema(), (byte[]) outputRecord.getValue());

        assertTrue(outputRecord.getKey().isPresent());
        assertEquals(outputRecord.getKey().get(), "new");
        assertEquals(read.get("firstName"), new Utf8("Jane"));
    }

    @Test
    void testAvroComputeHeaderProperties() throws Exception {
        RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("record");
        recordSchemaBuilder.field("firstName").type(SchemaType.STRING);

        SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.AVRO);
        GenericSchema<GenericRecord> genericSchema = Schema.generic(schemaInfo);

        GenericRecord genericRecord =
                genericSchema.newRecordBuilder().set("firstName", "Jane").build();

        Record<GenericObject> record =
                Utils.TestRecord.<GenericObject>builder()
                        .schema(genericSchema)
                        .value(genericRecord)
                        .properties(Map.of("existingKey", "v1", "nonExistingKey", "v2"))
                        .build();

        List<ComputeField> fields = new ArrayList<>();
        fields.add(
                ComputeField.builder()
                        .scopedName("properties.existingKey")
                        .expression("'c1'")
                        .type(ComputeFieldType.STRING)
                        .build());
        fields.add(
                ComputeField.builder()
                        .scopedName("properties.newKey")
                        .expression("'c2'")
                        .type(ComputeFieldType.STRING)
                        .build());
        ComputeStep step = ComputeStep.builder().fields(fields).build();
        Record<?> outputRecord = Utils.process(record, step);

        assertEquals(outputRecord.getProperties().size(), 3);
        assertTrue(outputRecord.getProperties().containsKey("existingKey"));
        assertTrue(outputRecord.getProperties().containsKey("nonExistingKey"));
        assertTrue(outputRecord.getProperties().containsKey("newKey"));
        assertEquals(outputRecord.getProperties().get("existingKey"), "c1");
        assertEquals(outputRecord.getProperties().get("nonExistingKey"), "v2");
        assertEquals(outputRecord.getProperties().get("newKey"), "c2");
    }

    public static Object[] destinationTopicProvider() {
        return new Object[] {"targetTopic", "randomTopic"};
    }

    /**
     * @return {"expression", "new schema", "expected value", "expectedSchema"}
     */
    public static Object[][] primitiveSchemaTypesProvider() {
        return new Object[][] {
            {"'newValue'", ComputeFieldType.STRING, "newValue", Schema.STRING},
            {"'4'", ComputeFieldType.INT8, (byte) 4, Schema.INT8},
            {"'4'", ComputeFieldType.INT16, (short) 4, Schema.INT16},
            {"'4'", ComputeFieldType.INT32, 4, Schema.INT32},
            {"'4'", ComputeFieldType.INT64, 4L, Schema.INT64},
            {"'3.2'", ComputeFieldType.FLOAT, 3.2F, Schema.FLOAT},
            {"'3.2'", ComputeFieldType.DOUBLE, 3.2D, Schema.DOUBLE},
            {"true", ComputeFieldType.BOOLEAN, true, Schema.BOOL},
            {"'2008-02-07'", ComputeFieldType.DATE, LocalDate.parse("2008-02-07"), Schema.DATE},
            {
                "'2008-02-07'",
                ComputeFieldType.LOCAL_DATE,
                LocalDate.parse("2008-02-07"),
                Schema.LOCAL_DATE
            },
            {"'03:04:05'", ComputeFieldType.TIME, Time.valueOf("03:04:05"), Schema.TIME},
            {
                "'03:04:05'",
                ComputeFieldType.LOCAL_TIME,
                LocalTime.parse("03:04:05"),
                Schema.LOCAL_TIME
            },
            {
                "'2009-01-02T01:02:03Z'",
                ComputeFieldType.INSTANT,
                Instant.parse("2009-01-02T01:02:03Z"),
                Schema.INSTANT
            },
            {
                "'2009-01-02T01:02:03Z'",
                ComputeFieldType.DATETIME,
                Instant.parse("2009-01-02T01:02:03Z"),
                Schema.INSTANT
            },
            {
                "'2009-01-02T01:02:03Z'",
                ComputeFieldType.TIMESTAMP,
                Timestamp.from(Instant.parse("2009-01-02T01:02:03Z")),
                Schema.TIMESTAMP
            },
            {
                "'2009-01-02T01:02:03'",
                ComputeFieldType.LOCAL_DATE_TIME,
                LocalDateTime.parse("2009-01-02T01:02:03"),
                Schema.LOCAL_DATE_TIME
            },
            {
                "'newValue'.bytes",
                ComputeFieldType.BYTES,
                "newValue".getBytes(StandardCharsets.UTF_8),
                Schema.BYTES
            },
        };
    }

    public static Object[][] primitiveInferredSchemaTypesProvider() {
        LocalDateTime now = LocalDateTime.now();
        return new Object[][] {
            {"", Schema.STRING, "'newValue'", "newValue", Schema.STRING},
            {"", Schema.STRING, "42", 42L, Schema.INT64},
            {"", Schema.STRING, "42.0", 42.0D, Schema.DOUBLE},
            {"", Schema.STRING, "true", true, Schema.BOOL},
            {
                "",
                Schema.STRING,
                "'newValue'.bytes",
                "newValue".getBytes(StandardCharsets.UTF_8),
                Schema.BYTES
            },
            {(byte) 42, Schema.INT8, "value", (byte) 42, Schema.INT8},
            {(short) 42, Schema.INT16, "value", (short) 42, Schema.INT16},
            {42, Schema.INT32, "value", 42, Schema.INT32},
            {new Date(42), Schema.DATE, "value", new Date(42), Schema.DATE},
            {new Timestamp(42), Schema.TIMESTAMP, "value", new Timestamp(42), Schema.TIMESTAMP},
            {new Time(42), Schema.TIME, "value", new Time(42), Schema.TIME},
            {now, Schema.LOCAL_DATE_TIME, "value", now, Schema.LOCAL_DATE_TIME},
            {now.toLocalDate(), Schema.LOCAL_DATE, "value", now.toLocalDate(), Schema.LOCAL_DATE},
            {now.toLocalTime(), Schema.LOCAL_TIME, "value", now.toLocalTime(), Schema.LOCAL_TIME},
            {
                now.toInstant(ZoneOffset.UTC),
                Schema.INSTANT,
                "value",
                now.toInstant(ZoneOffset.UTC),
                Schema.INSTANT
            },
        };
    }

    private void assertOptionalFieldNull(
            GenericData.Record record, String fieldName, org.apache.avro.Schema.Type expectedType) {
        assertOptionalField(record, fieldName, expectedType, null);
    }

    private List<ComputeField> buildComputeFields(
            String scope, boolean optional, boolean nullify, boolean inferType) {
        List<ComputeField> fields = new ArrayList<>();
        fields.add(
                ComputeField.builder()
                        .scopedName(scope + "." + "newStringField")
                        .expression(nullify ? "null" : "'Hotaru'")
                        .optional(optional)
                        .type(inferType ? null : ComputeFieldType.STRING)
                        .build());
        fields.add(
                ComputeField.builder()
                        .scopedName(scope + "." + "newInt8Field")
                        .expression(nullify ? "null" : "127")
                        .optional(optional)
                        .type(inferType ? null : ComputeFieldType.INT8)
                        .build());
        fields.add(
                ComputeField.builder()
                        .scopedName(scope + "." + "newInt16Field")
                        .expression(nullify ? "null" : "32767")
                        .optional(optional)
                        .type(inferType ? null : ComputeFieldType.INT16)
                        .build());
        fields.add(
                ComputeField.builder()
                        .scopedName(scope + "." + "newInt32Field")
                        .expression(nullify ? "null" : "2147483647")
                        .optional(optional)
                        .type(inferType ? null : ComputeFieldType.INT32)
                        .build());
        fields.add(
                ComputeField.builder()
                        .scopedName(scope + "." + "newInt64Field")
                        .expression(nullify ? "null" : "9223372036854775807")
                        .optional(optional)
                        .type(inferType ? null : ComputeFieldType.INT64)
                        .build());
        fields.add(
                ComputeField.builder()
                        .scopedName(scope + "." + "newFloatField")
                        .expression(
                                nullify ? "null" : "340282346638528859999999999999999999999.999999")
                        .optional(optional)
                        .type(inferType ? null : ComputeFieldType.FLOAT)
                        .build());
        fields.add(
                ComputeField.builder()
                        .scopedName(scope + "." + "newDoubleField")
                        .expression(nullify ? "null" : "1.79769313486231570e+308")
                        .optional(optional)
                        .type(inferType ? null : ComputeFieldType.DOUBLE)
                        .build());
        fields.add(
                ComputeField.builder()
                        .scopedName(scope + "." + "newBooleanField")
                        .expression(nullify ? "null" : "1 == 1")
                        .optional(optional)
                        .type(inferType ? null : ComputeFieldType.BOOLEAN)
                        .build());
        fields.add(
                ComputeField.builder()
                        .scopedName(scope + "." + "newDateField")
                        .expression(nullify ? "null" : "'2007-12-03'")
                        .optional(optional)
                        .type(inferType ? null : ComputeFieldType.DATE)
                        .build());
        fields.add(
                ComputeField.builder()
                        .scopedName(scope + "." + "newDateField2")
                        .expression(nullify ? "null" : "13850")
                        .optional(optional)
                        .type(inferType ? null : ComputeFieldType.DATE)
                        .build());
        fields.add(
                ComputeField.builder()
                        .scopedName(scope + "." + "newLocalDateField")
                        .expression(nullify ? "null" : "'2007-12-03'")
                        .optional(optional)
                        .type(inferType ? null : ComputeFieldType.LOCAL_DATE)
                        .build());
        fields.add(
                ComputeField.builder()
                        .scopedName(scope + "." + "newLocalDateField2")
                        .expression(nullify ? "null" : "13850")
                        .optional(optional)
                        .type(inferType ? null : ComputeFieldType.DATE)
                        .build());
        fields.add(
                ComputeField.builder()
                        .scopedName(scope + "." + "newTimeField")
                        .expression(nullify ? "null" : "'10:15:30'")
                        .optional(optional)
                        .type(inferType ? null : ComputeFieldType.TIME)
                        .build());
        fields.add(
                ComputeField.builder()
                        .scopedName(scope + "." + "newTimeField2")
                        .expression(nullify ? "null" : "36930000")
                        .optional(optional)
                        .type(inferType ? null : ComputeFieldType.TIME)
                        .build());
        fields.add(
                ComputeField.builder()
                        .scopedName(scope + "." + "newLocalTimeField")
                        .expression(nullify ? "null" : "'10:15:30'")
                        .optional(optional)
                        .type(inferType ? null : ComputeFieldType.LOCAL_TIME)
                        .build());
        fields.add(
                ComputeField.builder()
                        .scopedName(scope + "." + "newLocalTimeField2")
                        .expression(nullify ? "null" : "36930000")
                        .optional(optional)
                        .type(inferType ? null : ComputeFieldType.TIME)
                        .build());
        fields.add(
                ComputeField.builder()
                        .scopedName(scope + "." + "newTimestampField")
                        .expression(nullify ? "null" : "'2007-12-03T10:15:30+00:00'")
                        .optional(optional)
                        .type(inferType ? null : ComputeFieldType.INSTANT)
                        .build());
        fields.add(
                ComputeField.builder()
                        .scopedName(scope + "." + "newTimestampField2")
                        .expression(nullify ? "null" : "1196676930000")
                        .optional(optional)
                        .type(inferType ? null : ComputeFieldType.INSTANT)
                        .build());
        fields.add(
                ComputeField.builder()
                        .scopedName(scope + "." + "newInstantField")
                        .expression(nullify ? "null" : "'2007-12-03T10:15:30+00:00'")
                        .optional(optional)
                        .type(inferType ? null : ComputeFieldType.TIMESTAMP)
                        .build());
        fields.add(
                ComputeField.builder()
                        .scopedName(scope + "." + "newInstantField2")
                        .expression(nullify ? "null" : "1196676930000")
                        .optional(optional)
                        .type(inferType ? null : ComputeFieldType.INSTANT)
                        .build());
        fields.add(
                ComputeField.builder()
                        .scopedName(scope + "." + "newLocalDateTimeField")
                        .expression(nullify ? "null" : "'2007-12-03T10:15:30'")
                        .optional(optional)
                        .type(inferType ? null : ComputeFieldType.LOCAL_DATE_TIME)
                        .build());
        fields.add(
                ComputeField.builder()
                        .scopedName(scope + "." + "newLocalDateTimeField2")
                        .expression(nullify ? "null" : "1196676930000")
                        .optional(optional)
                        .type(inferType ? null : ComputeFieldType.INSTANT)
                        .build());
        fields.add(
                ComputeField.builder()
                        .scopedName(scope + "." + "newDateTimeField")
                        .expression(nullify ? "null" : "'2007-12-03T10:15:30+00:00'")
                        .optional(optional)
                        .type(inferType ? null : ComputeFieldType.DATETIME)
                        .build());
        fields.add(
                ComputeField.builder()
                        .scopedName(scope + "." + "newBytesField")
                        .expression(nullify ? "null" : "'Hotaru'.bytes")
                        .optional(optional)
                        .type(inferType ? null : ComputeFieldType.BYTES)
                        .build());

        return fields;
    }

    private org.apache.avro.generic.GenericRecord createDecimalRecord(BigDecimal decimal) {
        decimal.unscaledValue().toByteArray();
        org.apache.avro.generic.GenericRecord decimalRecord = new GenericData.Record(decimalType);
        decimalRecord.put(
                CQL_DECIMAL_BIGINT, ByteBuffer.wrap(decimal.unscaledValue().toByteArray()));
        decimalRecord.put(CQL_DECIMAL_SCALE, decimal.scale());
        return decimalRecord;
    }

    private org.apache.avro.Schema.Field createDecimalField(String name) {
        org.apache.avro.Schema.Field decimalField =
                new org.apache.avro.Schema.Field(name, decimalType);
        org.apache.avro.Schema.Field optionalDecimalField =
                new org.apache.avro.Schema.Field(
                        name,
                        org.apache.avro.SchemaBuilder.unionOf()
                                .nullType()
                                .and()
                                .type(decimalField.schema())
                                .endUnion(),
                        null,
                        org.apache.avro.Schema.Field.NULL_DEFAULT_VALUE);

        return optionalDecimalField;
    }

    private static class CqlDecimalLogicalType extends LogicalType {
        public CqlDecimalLogicalType() {
            super(CQL_DECIMAL);
        }
    }
}
