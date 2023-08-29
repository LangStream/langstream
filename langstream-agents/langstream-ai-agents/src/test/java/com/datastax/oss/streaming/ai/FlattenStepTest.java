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

import static com.datastax.oss.streaming.ai.FlattenStep.AVRO_READ_OFFSET_PROP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroRecord;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.junit.jupiter.api.Test;

public class FlattenStepTest {

    @Test
    void testRegularKeyValueNotModified() throws Exception {
        // given
        Record<GenericObject> record = Utils.createTestAvroKeyValueRecord();

        // when
        Record<?> outputRecord = Utils.process(record, FlattenStep.builder().build());

        // then (key & value remain unchanged)
        KeyValueSchema<?, ?> messageSchema = (KeyValueSchema<?, ?>) outputRecord.getSchema();
        KeyValue<?, ?> messageValue = (KeyValue<?, ?>) outputRecord.getValue();

        GenericData.Record keyRecord =
                Utils.getRecord(messageSchema.getKeySchema(), (byte[]) messageValue.getKey());
        GenericData.Record valueRecord =
                Utils.getRecord(messageSchema.getValueSchema(), (byte[]) messageValue.getValue());

        assertEquals(keyRecord.getSchema().getFields().size(), 3);
        assertEquals(keyRecord.get("keyField1"), new Utf8("key1"));
        assertEquals(keyRecord.get("keyField2"), new Utf8("key2"));
        assertEquals(keyRecord.get("keyField3"), new Utf8("key3"));

        assertEquals(valueRecord.getSchema().getFields().size(), 3);
        assertEquals(valueRecord.get("valueField1"), new Utf8("value1"));
        assertEquals(valueRecord.get("valueField2"), new Utf8("value2"));
        assertEquals(valueRecord.get("valueField3"), new Utf8("value3"));

        assertEquals(messageSchema.getKeyValueEncodingType(), KeyValueEncodingType.SEPARATED);
    }

    @Test
    void testNestedKeyValueFlattened() throws Exception {
        // given
        Record<GenericObject> nestedKVRecord = Utils.createNestedAvroKeyValueRecord(4);

        // when
        Record<?> outputRecord = Utils.process(nestedKVRecord, FlattenStep.builder().build());

        // then
        KeyValueSchema<?, ?> messageSchema = (KeyValueSchema<?, ?>) outputRecord.getSchema();
        KeyValue<?, ?> messageValue = (KeyValue<?, ?>) outputRecord.getValue();

        GenericData.Record keyRecord =
                Utils.getRecord(messageSchema.getKeySchema(), (byte[]) messageValue.getKey());
        GenericData.Record valueRecord =
                Utils.getRecord(messageSchema.getValueSchema(), (byte[]) messageValue.getValue());

        // Assert value flattened
        GenericData.Record key =
                (GenericData.Record)
                        ((GenericAvroRecord)
                                        ((KeyValue<?, ?>)
                                                        nestedKVRecord.getValue().getNativeObject())
                                                .getKey())
                                .getAvroRecord();
        assertSchemasFlattened(keyRecord, key);
        assertValuesFlattened(keyRecord, key);

        // Assert value flattened
        GenericData.Record value =
                (GenericData.Record)
                        ((GenericAvroRecord)
                                        ((KeyValue<?, ?>)
                                                        nestedKVRecord.getValue().getNativeObject())
                                                .getValue())
                                .getAvroRecord();
        assertSchemasFlattened(valueRecord, value);
        assertValuesFlattened(valueRecord, value);

        assertEquals(messageSchema.getKeyValueEncodingType(), KeyValueEncodingType.SEPARATED);
    }

    @Test
    void testNestedValueFlattened() throws Exception {
        // given
        Record<GenericObject> record = Utils.createNestedAvroRecord(4, "myKey");

        // when
        Record<?> outputRecord = Utils.process(record, FlattenStep.builder().part("value").build());

        // then
        assertEquals(outputRecord.getKey(), Optional.of("myKey"));

        GenericData.Record valueRecord =
                Utils.getRecord(outputRecord.getSchema(), (byte[]) outputRecord.getValue());

        // Assert value flattened
        GenericData.Record value = (GenericData.Record) record.getValue().getNativeObject();
        assertSchemasFlattened(valueRecord, value);
        assertValuesFlattened(valueRecord, value);
    }

    @Test
    void testNestedKeyValueFlattenedWithCustomDelimiter() throws Exception {
        // given
        Record<GenericObject> nestedKVRecord = Utils.createNestedAvroKeyValueRecord(4);

        // when
        Record<?> outputRecord =
                Utils.process(
                        nestedKVRecord,
                        FlattenStep.builder().delimiter("_CUSTOM_DELIMITER_").build());

        // then
        KeyValueSchema<?, ?> messageSchema = (KeyValueSchema<?, ?>) outputRecord.getSchema();
        KeyValue<?, ?> messageValue = (KeyValue<?, ?>) outputRecord.getValue();

        GenericData.Record keyRecord =
                Utils.getRecord(messageSchema.getKeySchema(), (byte[]) messageValue.getKey());
        GenericData.Record valueRecord =
                Utils.getRecord(messageSchema.getValueSchema(), (byte[]) messageValue.getValue());

        // Assert value flattened
        GenericData.Record key =
                (GenericData.Record)
                        ((GenericAvroRecord)
                                        ((KeyValue<?, ?>)
                                                        nestedKVRecord.getValue().getNativeObject())
                                                .getKey())
                                .getAvroRecord();
        assertSchemasFlattened(keyRecord, key, "_CUSTOM_DELIMITER_");
        assertValuesFlattened(keyRecord, key, "_CUSTOM_DELIMITER_");

        // Assert value flattened
        GenericData.Record value =
                (GenericData.Record)
                        ((GenericAvroRecord)
                                        ((KeyValue<?, ?>)
                                                        nestedKVRecord.getValue().getNativeObject())
                                                .getValue())
                                .getAvroRecord();
        assertSchemasFlattened(valueRecord, value, "_CUSTOM_DELIMITER_");
        assertValuesFlattened(valueRecord, value, "_CUSTOM_DELIMITER_");

        assertEquals(messageSchema.getKeyValueEncodingType(), KeyValueEncodingType.SEPARATED);
    }

    @Test
    void testNestedKeyValueKeyOnlyFlattened() throws Exception {
        // given
        Record<GenericObject> nestedKVRecord = Utils.createNestedAvroKeyValueRecord(4);

        // when
        Record<?> outputRecord =
                Utils.process(nestedKVRecord, FlattenStep.builder().part("key").build());

        // then
        KeyValueSchema<?, ?> messageSchema = (KeyValueSchema<?, ?>) outputRecord.getSchema();
        KeyValue<?, ?> messageValue = (KeyValue<?, ?>) outputRecord.getValue();

        GenericData.Record keyRecord =
                Utils.getRecord(messageSchema.getKeySchema(), (byte[]) messageValue.getKey());
        GenericData.Record valueRecord =
                Utils.getRecord(messageSchema.getValueSchema(), (byte[]) messageValue.getValue());

        // Assert key flattened
        GenericData.Record key =
                (GenericData.Record)
                        ((GenericAvroRecord)
                                        ((KeyValue<?, ?>)
                                                        nestedKVRecord.getValue().getNativeObject())
                                                .getKey())
                                .getAvroRecord();
        assertSchemasFlattened(keyRecord, key);
        assertValuesFlattened(keyRecord, key);

        // Assert value unchanged
        GenericData.Record value =
                (GenericData.Record)
                        ((GenericAvroRecord)
                                        ((KeyValue<?, ?>)
                                                        nestedKVRecord.getValue().getNativeObject())
                                                .getValue())
                                .getAvroRecord();
        assertEquals(valueRecord, value);

        assertEquals(messageSchema.getKeyValueEncodingType(), KeyValueEncodingType.SEPARATED);
    }

    @Test
    void testNestedKeyValueValueOnlyFlattened() throws Exception {
        // given
        Record<GenericObject> nestedKVRecord = Utils.createNestedAvroKeyValueRecord(4);

        // when
        Record<?> outputRecord =
                Utils.process(nestedKVRecord, FlattenStep.builder().part("value").build());

        // then
        KeyValueSchema<?, ?> messageSchema = (KeyValueSchema<?, ?>) outputRecord.getSchema();
        KeyValue<?, ?> messageValue = (KeyValue<?, ?>) outputRecord.getValue();

        GenericData.Record keyRecord =
                Utils.getRecord(messageSchema.getKeySchema(), (byte[]) messageValue.getKey());
        GenericData.Record valueRecord =
                Utils.getRecord(messageSchema.getValueSchema(), (byte[]) messageValue.getValue());

        // Assert key unchanged
        GenericData.Record key =
                (GenericData.Record)
                        ((GenericAvroRecord)
                                        ((KeyValue<?, ?>)
                                                        nestedKVRecord.getValue().getNativeObject())
                                                .getKey())
                                .getAvroRecord();
        assertEquals(keyRecord, key);

        // Assert value flattened
        GenericData.Record value =
                (GenericData.Record)
                        ((GenericAvroRecord)
                                        ((KeyValue<?, ?>)
                                                        nestedKVRecord.getValue().getNativeObject())
                                                .getValue())
                                .getAvroRecord();
        assertSchemasFlattened(valueRecord, value);
        assertValuesFlattened(valueRecord, value);
    }

    @Test
    void testNestedKeyValueInvalidType() throws Exception {
        assertEquals(
                "Unsupported part for Flatten: invalid",
                assertThrows(
                                IllegalArgumentException.class,
                                () -> {
                                    // given
                                    Record<GenericObject> nestedKVRecord =
                                            Utils.createNestedAvroKeyValueRecord(4);

                                    // then
                                    Utils.process(
                                            nestedKVRecord,
                                            FlattenStep.builder().part("invalid").build());
                                })
                        .getMessage());
    }

    @Test
    void testNestedKeyValueNonAvroSchema() throws Exception {
        assertEquals(
                "Unsupported schema type for Flatten: JSON",
                assertThrows(
                                IllegalStateException.class,
                                () -> {
                                    // given
                                    Record<GenericObject> nestedKVRecord =
                                            Utils.createNestedJSONRecord(4, "myKey");

                                    // then
                                    Utils.process(
                                            nestedKVRecord,
                                            FlattenStep.builder().part("value").build());
                                })
                        .getMessage());
    }

    @Test
    void testNestedSchemalessValue() throws Exception {
        assertEquals(
                "Flatten requires non-null schemas!",
                assertThrows(
                                IllegalStateException.class,
                                () -> {
                                    // given
                                    Record<GenericObject> nestedKVRecord =
                                            new Utils.TestRecord<>(
                                                    null,
                                                    AutoConsumeSchema.wrapPrimitiveObject(
                                                            "value",
                                                            SchemaType.STRING,
                                                            new byte[] {}),
                                                    "myKey");

                                    // then
                                    Utils.process(
                                            nestedKVRecord,
                                            FlattenStep.builder().part("value").build());
                                })
                        .getMessage());
    }

    private void assertSchemasFlattened(
            GenericData.Record actual, GenericData.Record expected, String delimiter) {
        assertEquals(actual.getSchema().getFields().size(), 11);
        assertTopLevelSchema(actual.getSchema(), expected.getSchema());
        assertField(
                actual.getSchema().getField("level1String"),
                expected.getSchema().getField("level1String"));
        Schema.Field expectedL1 = expected.getSchema().getField("level1Record");
        assertField(
                actual.getSchema()
                        .getField(String.format("level1Record%1$slevel2String", delimiter)),
                expectedL1.schema().getField("level2String"));
        Schema.Field expectedL2 = expectedL1.schema().getField("level2Record");
        assertField(
                actual.getSchema()
                        .getField(
                                String.format(
                                        "level1Record%1$slevel2Record%1$slevel3String", delimiter)),
                expectedL2.schema().getField("level3String"));
        Schema.Field expectedL3 = expectedL2.schema().getField("level3Record");
        assertField(
                actual.getSchema()
                        .getField(
                                String.format(
                                        "level1Record%1$slevel2Record%1$slevel3Record%1$slevel4String",
                                        delimiter)),
                expectedL3.schema().getField("level4String"));
        assertField(
                actual.getSchema()
                        .getField(
                                String.format(
                                        "level1Record%1$slevel2Record%1$slevel3Record%1$slevel4Integer",
                                        delimiter)),
                expectedL3.schema().getField("level4Integer"));
        assertField(
                actual.getSchema()
                        .getField(
                                String.format(
                                        "level1Record%1$slevel2Record%1$slevel3Record%1$slevel4Double",
                                        delimiter)),
                expectedL3.schema().getField("level4Double"));
        assertField(
                actual.getSchema()
                        .getField(
                                String.format(
                                        "level1Record%1$slevel2Record%1$slevel3Record%1$slevel4StringWithPropsAndAlias",
                                        delimiter)),
                expectedL3.schema().getField("level4StringWithPropsAndAlias"));
        assertField(
                actual.getSchema()
                        .getField(
                                String.format(
                                        "level1Record%1$slevel2Record%1$slevel3Record%1$slevel4Union",
                                        delimiter)),
                expectedL3.schema().getField("level4Union"));
    }

    private void assertSchemasFlattened(GenericData.Record actual, GenericData.Record expected) {
        assertSchemasFlattened(actual, expected, "_");
    }

    private void assertValuesFlattened(
            GenericData.Record actual, GenericData.Record expected, String delimiter) {
        assertEquals(actual.getSchema().getFields().size(), 11);

        assertEquals(actual.get("level1String"), new Utf8((String) expected.get("level1String")));
        GenericData.Record expectedL1 = (GenericData.Record) expected.get("level1Record");
        assertEquals(
                actual.get(String.format("level1Record%1$slevel2String", delimiter)),
                new Utf8((String) expectedL1.get("level2String")));
        GenericData.Record expectedL2 = (GenericData.Record) expectedL1.get("level2Record");
        assertEquals(
                actual.get(
                        String.format("level1Record%1$slevel2Record%1$slevel3String", delimiter)),
                new Utf8((String) expectedL2.get("level3String")));
        GenericData.Record expectedL3 = (GenericData.Record) expectedL2.get("level3Record");
        assertEquals(
                actual.get(
                        String.format(
                                "level1Record%1$slevel2Record%1$slevel3Record%1$slevel4String",
                                delimiter)),
                new Utf8((String) expectedL3.get("level4String")));
        assertEquals(
                actual.get(
                        String.format(
                                "level1Record%1$slevel2Record%1$slevel3Record%1$slevel4Integer",
                                delimiter)),
                expectedL3.get("level4Integer"));
        assertEquals(
                actual.get(
                        String.format(
                                "level1Record%1$slevel2Record%1$slevel3Record%1$slevel4Double",
                                delimiter)),
                expectedL3.get("level4Double"));
        assertEquals(
                actual.get(
                        String.format(
                                "level1Record%1$slevel2Record%1$slevel3Record%1$slevel4StringWithPropsAndAlias",
                                delimiter)),
                new Utf8((String) expectedL3.get("level4StringWithPropsAndAlias")));
        assertEquals(
                actual.get(
                        String.format(
                                "level1Record%1$slevel2Record%1$slevel3Record%1$slevel4Union",
                                delimiter)),
                new Utf8((String) expectedL3.get("level4Union")));
        assertNull(
                actual.get(
                        String.format(
                                "level1Record%1$slevel2Record%1$slevel3Record%1$slevel4Null",
                                delimiter)));
        assertNull(
                actual.get(
                        String.format(
                                "level1Record%1$slevel2Record%1$slevel3Record%1$slevel4NullRecord%1$signored",
                                delimiter)));
    }

    private void assertValuesFlattened(GenericData.Record actual, GenericData.Record expected) {
        assertValuesFlattened(actual, expected, "_");
    }

    private void assertField(Schema.Field actual, Schema.Field expected) {
        assertEquals(actual.schema(), expected.schema());
        assertEquals(actual.doc(), expected.doc());
        assertEquals(actual.defaultVal(), expected.defaultVal());
        assertEquals(actual.getObjectProps(), expected.getObjectProps());
        assertEquals(actual.aliases(), expected.aliases());
    }

    private void assertTopLevelSchema(Schema actual, Schema expected) {
        assertEquals(actual.getName(), expected.getName());
        assertEquals(actual.getNamespace(), expected.getNamespace());
        assertEquals(actual.getDoc(), expected.getDoc());
        assertEquals(
                actual.getObjectProps(),
                expected.getObjectProps().entrySet().stream()
                        .filter(e -> !AVRO_READ_OFFSET_PROP.equals(e.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }
}
