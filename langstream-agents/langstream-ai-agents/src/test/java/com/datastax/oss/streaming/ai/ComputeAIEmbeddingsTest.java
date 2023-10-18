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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import ai.langstream.ai.agents.commons.MutableRecord;
import ai.langstream.api.runner.code.SimpleRecord;
import com.datastax.oss.streaming.ai.embeddings.MockEmbeddingsService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.junit.jupiter.api.Test;

public class ComputeAIEmbeddingsTest {

    @Test
    void testAvro() throws Exception {
        RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("record");
        recordSchemaBuilder.field("firstName").type(SchemaType.STRING);
        recordSchemaBuilder.field("lastName").type(SchemaType.STRING);

        SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.AVRO);
        GenericSchema<GenericRecord> genericSchema = Schema.generic(schemaInfo);

        GenericRecord genericRecord =
                genericSchema
                        .newRecordBuilder()
                        .set("firstName", "Jane")
                        .set("lastName", "The Princess ")
                        .build();

        Record<GenericObject> record =
                new Utils.TestRecord<>(genericSchema, genericRecord, "test-key");
        MockEmbeddingsService mockService = new MockEmbeddingsService();
        final List<Double> expectedEmbeddings = Arrays.asList(1.0d, 2.0d, 3.0d);
        mockService.setEmbeddingsForText("Jane The Princess ", expectedEmbeddings);
        ComputeAIEmbeddingsStep step =
                new ComputeAIEmbeddingsStep(
                        "{{ value.firstName }} {{ value.lastName }}",
                        "value.newField",
                        null,
                        1,
                        500,
                        1,
                        mockService);

        Record<?> outputRecord = Utils.process(record, step);
        GenericData.Record read =
                Utils.getRecord(outputRecord.getSchema(), (byte[]) outputRecord.getValue());
        assertNotNull(read.get("newField"));
        List<Double> embeddings = (List<Double>) read.get("newField");
        assertEquals(embeddings, expectedEmbeddings);
        assertEquals(outputRecord.getSchema().getSchemaInfo().getType(), SchemaType.AVRO);
    }

    @Test
    void testKeyValueAvro() throws Exception {
        MockEmbeddingsService mockService = new MockEmbeddingsService();
        final List<Double> expectedEmbeddings = Arrays.asList(1.0d, 2.0d, 3.0d);
        mockService.setEmbeddingsForText("key1", expectedEmbeddings);
        ComputeAIEmbeddingsStep step =
                new ComputeAIEmbeddingsStep(
                        "{{ key.keyField1 }}", "value.newField", null, 1, 500, 1, mockService);

        Record<?> outputRecord = Utils.process(Utils.createTestAvroKeyValueRecord(), step);
        KeyValueSchema<?, ?> messageSchema = (KeyValueSchema<?, ?>) outputRecord.getSchema();
        KeyValue<?, ?> messageValue = (KeyValue<?, ?>) outputRecord.getValue();

        GenericData.Record valueAvroRecord =
                Utils.getRecord(messageSchema.getValueSchema(), (byte[]) messageValue.getValue());
        assertEquals(valueAvroRecord.get("newField"), expectedEmbeddings);
        assertEquals(outputRecord.getSchema().getSchemaInfo().getType(), SchemaType.KEY_VALUE);
    }

    @Test
    void testJson() throws Exception {
        RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("record");
        recordSchemaBuilder.field("firstName").type(SchemaType.STRING);
        recordSchemaBuilder.field("lastName").type(SchemaType.STRING);

        SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.JSON);
        GenericSchema<GenericRecord> genericSchema = Schema.generic(schemaInfo);

        GenericRecord genericRecord =
                genericSchema
                        .newRecordBuilder()
                        .set("firstName", "Jane")
                        .set("lastName", "The Princess ")
                        .build();

        Record<GenericObject> record =
                new Utils.TestRecord<>(genericSchema, genericRecord, "test-key");
        MockEmbeddingsService mockService = new MockEmbeddingsService();
        final List<Double> expectedEmbeddings = Arrays.asList(1.0d, 2.0d, 3.0d);
        mockService.setEmbeddingsForText("Jane The Princess ", expectedEmbeddings);
        ComputeAIEmbeddingsStep step =
                new ComputeAIEmbeddingsStep(
                        "{{ value.firstName }} {{ value.lastName }}",
                        "value.newField",
                        null,
                        1,
                        500,
                        1,
                        mockService);

        Record<?> outputRecord = Utils.process(record, step);

        final ObjectNode jsonNode = (ObjectNode) outputRecord.getValue();
        assertNotNull(jsonNode.get("newField"));
        final List asList = new ObjectMapper().convertValue(jsonNode.get("newField"), List.class);
        assertEquals(asList, expectedEmbeddings);
        assertEquals(outputRecord.getSchema().getSchemaInfo().getType(), SchemaType.JSON);
    }

    @Test
    void testLoopOver() throws Exception {
        MockEmbeddingsService mockService = new MockEmbeddingsService();
        mockService.setEmbeddingsForText("Jane The Princess", Arrays.asList(1.0d, 2.0d, 3.0d));
        mockService.setEmbeddingsForText("George The Prince", Arrays.asList(1.0d, 5.0d, 3.0d));
        ComputeAIEmbeddingsStep step =
                new ComputeAIEmbeddingsStep(
                        "{{ record.firstName }} {{ record.lastName }}",
                        "record.newField",
                        "value.documents_to_retrieve",
                        1,
                        0,
                        1,
                        mockService);

        SimpleRecord record =
                SimpleRecord.of(
                        null,
                        """
                {
                    "documents_to_retrieve": [
                       {
                            "firstName": "Jane",
                            "lastName": "The Princess"
                       },
                       {
                            "firstName": "George",
                            "lastName": "The Prince"
                       }
                    ]
                }
                """);

        MutableRecord mutableRecord = MutableRecord.recordToMutableRecord(record, true);
        step.process(mutableRecord);
        ai.langstream.api.runner.code.Record result =
                MutableRecord.mutableRecordToRecord(mutableRecord).orElseThrow();

        Object value = result.value();

        assertEquals(
                """
                {documents_to_retrieve=[{firstName=Jane, lastName=The Princess, newField=[1.0, 2.0, 3.0]}, {firstName=George, lastName=The Prince, newField=[1.0, 5.0, 3.0]}]}""",
                value.toString());
    }
}
