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

import ai.langstream.ai.agents.commons.MutableRecord;
import ai.langstream.api.runner.code.SimpleRecord;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.junit.jupiter.api.Test;

@Slf4j
public class QueryStepTest {

    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void testPrimitive() throws Exception {
        Record<GenericObject> record =
                Utils.TestRecord.<GenericObject>builder()
                        .key("test-key")
                        .value(
                                AutoConsumeSchema.wrapPrimitiveObject(
                                        "test-message", SchemaType.STRING, new byte[] {}))
                        .schema(Schema.STRING)
                        .eventTime(42L)
                        .topicName("test-input-topic")
                        .destinationTopic("test-output-topic")
                        .properties(Map.of("test-key", "test-value"))
                        .build();

        List<String> fields =
                Arrays.asList("value", "destinationTopic", "messageKey", "topicName", "eventTime");
        QueryStepDataSource dataSource =
                new QueryStepDataSource() {
                    @Override
                    public List<Map<String, Object>> fetchData(String query, List<Object> params) {
                        assertEquals(query, "select 1");
                        List<Object> expectedParams =
                                List.of(
                                        "test-message",
                                        "test-output-topic",
                                        "test-key",
                                        "test-input-topic",
                                        42L);
                        assertEquals(params, expectedParams);
                        return List.of(Map.of("foo", "bar"));
                    }
                };
        QueryStep queryStep =
                QueryStep.builder()
                        .dataSource(dataSource)
                        .outputFieldName("value")
                        .query("select 1")
                        .fields(fields)
                        .build();

        Record<Object> result = Utils.process(record, queryStep);
        assertEquals(List.of(Map.of("foo", "bar")), result.getValue());
    }

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
                        .build();

        Record<GenericObject> record =
                new Utils.TestRecord<>(genericSchema, genericRecord, "test-key");
        QueryStepDataSource dataSource =
                new QueryStepDataSource() {
                    @Override
                    public List<Map<String, Object>> fetchData(String query, List<Object> params) {
                        assertEquals(query, "select 1");
                        List<Object> expectedParams =
                                List.of(
                                        "Jane",
                                        "Doe",
                                        42,
                                        java.time.LocalDate.of(2023, 1, 2),
                                        Instant.ofEpochMilli(1672700645006L),
                                        LocalTime.parse("23:04:05.006"));
                        assertEquals(params, expectedParams);
                        return List.of(Map.of());
                    }
                };
        List<String> fields =
                Arrays.asList(
                        "value.firstName",
                        "value.lastName",
                        "value.age",
                        "value.date",
                        "value.timestamp",
                        "value.time");
        QueryStep queryStep =
                QueryStep.builder()
                        .dataSource(dataSource)
                        .outputFieldName("value.result")
                        .query("select 1")
                        .fields(fields)
                        .build();

        Utils.process(record, queryStep);
    }

    @Test
    void testJson() throws Exception {
        Schema<PoJo> schema = Schema.JSON(PoJo.class);
        GenericSchema<GenericRecord> genericSchema = Schema.generic(schema.getSchemaInfo());
        GenericObject genericObject =
                new GenericObject() {
                    @Override
                    public SchemaType getSchemaType() {
                        return SchemaType.JSON;
                    }

                    @Override
                    public Object getNativeObject() {
                        return OBJECT_MAPPER.valueToTree(
                                new PoJo("a", 42, true, List.of("b", "c"), List.of(43, 44)));
                    }
                };
        Record<GenericObject> record =
                new Utils.TestRecord<>(genericSchema, genericObject, "my-key");

        QueryStepDataSource dataSource =
                new QueryStepDataSource() {
                    @Override
                    public List<Map<String, Object>> fetchData(String query, List<Object> params) {
                        assertEquals(query, "select 1");
                        List<Object> expectedParams =
                                List.of("a", 42, true, List.of("b", "c"), List.of(43, 44));
                        assertEquals(params, expectedParams);
                        return List.of(Map.of());
                    }
                };

        List<String> fields =
                Arrays.asList(
                        "value.a", "value.b", "value.c", "value.stringList", "value.numberList");

        QueryStep queryStep =
                QueryStep.builder()
                        .dataSource(dataSource)
                        .outputFieldName("value.result")
                        .query("select 1")
                        .fields(fields)
                        .build();

        Utils.process(record, queryStep);
    }

    @Data
    @AllArgsConstructor
    static class PoJo {
        String a;
        Integer b;
        Boolean c;
        List<String> stringList;
        List<Integer> numberList;
    }

    @Test
    void testKVStringJson() throws Exception {
        Schema<KeyValue<String, String>> keyValueSchema =
                Schema.KeyValue(Schema.STRING, Schema.STRING, KeyValueEncodingType.SEPARATED);

        String key = "{\"keyField1\": \"key1\", \"keyField2\": \"key2\", \"keyField3\": \"key3\"}";
        String value =
                "{\"valueField1\": \"value1\", \"valueField2\": \"value2\", \"valueField3\": \"value3\"}";

        KeyValue<String, String> keyValue = new KeyValue<>(key, value);

        Record<GenericObject> record =
                new Utils.TestRecord<>(
                        keyValueSchema,
                        AutoConsumeSchema.wrapPrimitiveObject(
                                keyValue, SchemaType.KEY_VALUE, new byte[] {}),
                        null);

        QueryStepDataSource dataSource =
                new QueryStepDataSource() {
                    @Override
                    public List<Map<String, Object>> fetchData(String query, List<Object> params) {
                        assertEquals(query, "select 1");
                        List<Object> expectedParams = List.of("value1", "key2");
                        assertEquals(params, expectedParams);
                        return List.of(Map.of());
                    }
                };
        List<String> fields = Arrays.asList("value.valueField1", "key.keyField2");
        QueryStep queryStep =
                QueryStep.builder()
                        .dataSource(dataSource)
                        .outputFieldName("value.result")
                        .query("select 1")
                        .fields(fields)
                        .build();

        Utils.process(record, queryStep);
    }

    @Test
    void testKVAvro() throws Exception {
        Record<GenericObject> record = Utils.createTestAvroKeyValueRecord();
        QueryStepDataSource dataSource =
                new QueryStepDataSource() {
                    @Override
                    public List<Map<String, Object>> fetchData(String query, List<Object> params) {
                        assertEquals(query, "select 1");
                        List<Object> expectedParams = List.of("value1", "key2");
                        assertEquals(params, expectedParams);
                        return List.of(Map.of());
                    }
                };
        List<String> fields = Arrays.asList("value.valueField1", "key.keyField2");
        QueryStep queryStep =
                QueryStep.builder()
                        .dataSource(dataSource)
                        .outputFieldName("value.result")
                        .query("select 1")
                        .fields(fields)
                        .build();

        Utils.process(record, queryStep);
    }

    @Test
    void testOnlyFirst() throws Exception {
        Schema<KeyValue<String, String>> keyValueSchema =
                Schema.KeyValue(Schema.STRING, Schema.STRING, KeyValueEncodingType.SEPARATED);

        String key = "{\"keyField1\": \"key1\", \"keyField2\": \"key2\", \"keyField3\": \"key3\"}";
        String value =
                "{\"valueField1\": \"value1\", \"valueField2\": \"value2\", \"valueField3\": \"value3\"}";

        KeyValue<String, String> keyValue = new KeyValue<>(key, value);

        Record<GenericObject> record =
                new Utils.TestRecord<>(
                        keyValueSchema,
                        AutoConsumeSchema.wrapPrimitiveObject(
                                keyValue, SchemaType.KEY_VALUE, new byte[] {}),
                        null);

        List<String> fields = List.of();
        QueryStepDataSource dataSource =
                new QueryStepDataSource() {
                    @Override
                    public List<Map<String, Object>> fetchData(String query, List<Object> params) {
                        List<Object> expectedParams = List.of();
                        assertEquals(params, expectedParams);
                        switch (query) {
                            case "select a,b from test":
                                return List.of(
                                        Map.of("a", "10", "b", "foo"),
                                        Map.of("a", "20", "b", "bar"));
                            case "select a,b from test where 1=0":
                                return List.of(Map.of());
                            default:
                                throw new RuntimeException("Unexpected query: " + query);
                        }
                    }
                };

        QueryStep queryStepFindSomeResults =
                QueryStep.builder()
                        .dataSource(dataSource)
                        .outputFieldName("value.result")
                        .query("select a,b from test")
                        .onlyFirst(true)
                        .fields(fields)
                        .build();
        Record<?> result = Utils.process(record, queryStepFindSomeResults);
        KeyValue<String, String> keyValueResult = (KeyValue<String, String>) result.getValue();
        Map<String, Object> parsed =
                new ObjectMapper().readValue(keyValueResult.getValue(), Map.class);
        assertEquals(
                parsed,
                Map.of(
                        "valueField1", "value1",
                        "valueField2", "value2",
                        "valueField3", "value3",
                        "result", Map.of("b", "foo", "a", "10")));

        QueryStep queryStepFindNoResults =
                QueryStep.builder()
                        .dataSource(dataSource)
                        .outputFieldName("value.result")
                        .query("select a,b from test where 1=0")
                        .onlyFirst(true)
                        .fields(fields)
                        .build();
        Record<KeyValue<String, String>> resultNoResults =
                Utils.process(record, queryStepFindNoResults);
        KeyValue<String, String> keyValueResultNoResults = resultNoResults.getValue();
        Map<String, Object> parsedNoResults =
                new ObjectMapper().readValue(keyValueResultNoResults.getValue(), Map.class);
        assertEquals(
                parsedNoResults,
                Map.of(
                        "valueField1",
                        "value1",
                        "valueField2",
                        "value2",
                        "valueField3",
                        "value3",
                        "result",
                        Map.of()));
    }

    @Test
    void testLoopOver() throws Exception {

        String value =
                """
                {
                    "documents_to_retrieve": [
                        {
                            "text": "text 1",
                            "embeddings": [1,2,3,4,5]
                        },
                        {
                            "text": "text 2",
                            "embeddings": [2,2,3,4,5]
                        }
                    ]
                }
                """;

        QueryStepDataSource dataSource =
                new QueryStepDataSource() {
                    @Override
                    public List<Map<String, Object>> fetchData(String query, List<Object> params) {
                        log.info("Params: {}", params);
                        List<Double> param1 = (List<Double>) params.get(0);
                        if (param1.equals(List.of(1, 2, 3, 4, 5))) {
                            return List.of(
                                    Map.of("text", "retrieved-similar-to-1-1"),
                                    Map.of("text", "retrieved-similar-to-1-2"));
                        } else if (param1.equals(List.of(2, 2, 3, 4, 5))) {
                            return List.of(Map.of("text", "retrieved-similar-to-2"));
                        } else {
                            throw new IllegalArgumentException(params + "");
                        }
                    }
                };

        QueryStep queryStep =
                QueryStep.builder()
                        .dataSource(dataSource)
                        .loopOver("value.documents_to_retrieve")
                        .outputFieldName("value.retrieved_documents")
                        .fields(List.of("record.embeddings"))
                        .query("select 1 where vector near ?")
                        .build();
        queryStep.start();

        MutableRecord context =
                MutableRecord.recordToMutableRecord(SimpleRecord.of(null, value), true);

        queryStep.process(context);
        ai.langstream.api.runner.code.Record record =
                MutableRecord.mutableRecordToRecord(context).orElseThrow();
        Map<String, Object> result = (Map<String, Object>) record.value();
        log.info("Result: {}", result);

        List<Map<String, Object>> retrieved_documents =
                (List<Map<String, Object>>) result.get("retrieved_documents");
        assertEquals(
                List.of(
                        Map.of("text", "retrieved-similar-to-1-1"),
                        Map.of("text", "retrieved-similar-to-1-2"),
                        Map.of("text", "retrieved-similar-to-2")),
                retrieved_documents);
    }

    @Test
    void testExecute() throws Exception {

        String value = """
                {"question":"really?"}
                """;

        QueryStepDataSource dataSource =
                new QueryStepDataSource() {
                    @Override
                    public Map<String, Object> executeStatement(
                            String query, List<String> generatedKeys, List<Object> params) {
                        assertEquals(List.of("pk"), generatedKeys);
                        log.info("Params: {}", params);
                        Map<String, Object> res = new HashMap<>();
                        for (int i = 0; i < params.size(); i++) {
                            res.put(i + "", params.get(0));
                        }
                        return res;
                    }

                    @Override
                    public List<Map<String, Object>> fetchData(String query, List<Object> params) {
                        throw new UnsupportedOperationException();
                    }
                };

        QueryStep queryStep =
                QueryStep.builder()
                        .dataSource(dataSource)
                        .outputFieldName("value.command_results")
                        .fields(List.of("value.question"))
                        .mode("execute")
                        .generatedKeys(List.of("pk"))
                        .query("update something set a=1 WHERE value=?")
                        .build();
        queryStep.start();

        MutableRecord context =
                MutableRecord.recordToMutableRecord(SimpleRecord.of(null, value), true);

        queryStep.process(context);
        ai.langstream.api.runner.code.Record record =
                MutableRecord.mutableRecordToRecord(context).orElseThrow();
        Map<String, Object> result = (Map<String, Object>) record.value();
        log.info("Result: {}", result);

        Map<String, Object> command_results = (Map<String, Object>) result.get("command_results");
        assertEquals(Map.of("0", "really?"), command_results);
    }

    @Test
    void testLoopOverWithExecute() throws Exception {

        String value =
                """
                {
                    "documents_to_retrieve": [
                        {
                            "text": "text 1",
                            "embeddings": [1,2,3,4,5]
                        },
                        {
                            "text": "text 2",
                            "embeddings": [2,2,3,4,5]
                        }
                    ]
                }
                """;

        QueryStepDataSource dataSource =
                new QueryStepDataSource() {
                    @Override
                    public Map<String, Object> executeStatement(
                            String query, List<String> generatedKeys, List<Object> params) {
                        assertEquals(List.of("pk"), generatedKeys);
                        log.info("Params: {}", params);
                        List<Double> param1 = (List<Double>) params.get(0);
                        if (param1.equals(List.of(1, 2, 3, 4, 5))) {
                            return Map.of("foo", "bar");
                        } else if (param1.equals(List.of(2, 2, 3, 4, 5))) {
                            return Map.of("foo", "bar2");
                        } else {
                            throw new IllegalArgumentException(params + "");
                        }
                    }

                    @Override
                    public List<Map<String, Object>> fetchData(String query, List<Object> params) {
                        throw new UnsupportedOperationException();
                    }
                };

        QueryStep queryStep =
                QueryStep.builder()
                        .dataSource(dataSource)
                        .loopOver("value.documents_to_retrieve")
                        .outputFieldName("value.command_results")
                        .fields(List.of("record.embeddings"))
                        .mode("execute")
                        .generatedKeys(List.of("pk"))
                        .query("update something set a=1 WHERE value=?")
                        .build();
        queryStep.start();

        MutableRecord context =
                MutableRecord.recordToMutableRecord(SimpleRecord.of(null, value), true);

        queryStep.process(context);
        ai.langstream.api.runner.code.Record record =
                MutableRecord.mutableRecordToRecord(context).orElseThrow();
        Map<String, Object> result = (Map<String, Object>) record.value();
        log.info("Result: {}", result);

        List<Map<String, Object>> command_results =
                (List<Map<String, Object>>) result.get("command_results");
        assertEquals(List.of(Map.of("foo", "bar"), Map.of("foo", "bar2")), command_results);
    }

    @Test
    void testSetFieldWithDash() throws Exception {

        String value = "{}";

        QueryStepDataSource dataSource =
                new QueryStepDataSource() {

                    @Override
                    public List<Map<String, Object>> fetchData(String query, List<Object> params) {
                        return List.of(Map.of("foo", "bar"));
                    }
                };

        QueryStep queryStep =
                QueryStep.builder()
                        .dataSource(dataSource)
                        .outputFieldName("value.command-results")
                        .query("select 1")
                        .build();
        queryStep.start();

        MutableRecord context =
                MutableRecord.recordToMutableRecord(SimpleRecord.of(null, value), true);

        queryStep.process(context);
        ai.langstream.api.runner.code.Record record =
                MutableRecord.mutableRecordToRecord(context).orElseThrow();
        Map<String, Object> result = (Map<String, Object>) record.value();
        log.info("Result: {}", result);

        List<Map<String, Object>> command_results =
                (List<Map<String, Object>>) result.get("command-results");
        assertEquals(List.of(Map.of("foo", "bar")), command_results);
    }
}
