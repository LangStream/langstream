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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import ai.langstream.ai.agents.commons.TransformSchemaType;
import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.TimeZone;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class CastStepTest {

    @Test
    void testKeyValueAvroToString() throws Exception {
        Record<GenericObject> record = Utils.createTestAvroKeyValueRecord();
        CastStep step =
                CastStep.builder()
                        .keySchemaType(TransformSchemaType.STRING)
                        .valueSchemaType(TransformSchemaType.STRING)
                        .build();
        Record<?> outputRecord = Utils.process(record, step);

        KeyValueSchema<?, ?> messageSchema = (KeyValueSchema<?, ?>) outputRecord.getSchema();
        KeyValue<?, ?> messageValue = (KeyValue<?, ?>) outputRecord.getValue();

        assertSame(messageSchema.getKeySchema(), Schema.STRING);
        assertEquals(
                messageValue.getKey(),
                "{\"keyField1\": \"key1\", \"keyField2\": \"key2\", \"keyField3\": \"key3\"}");
        assertSame(messageSchema.getValueSchema(), Schema.STRING);
        assertEquals(
                messageValue.getValue(),
                "{\"valueField1\": \"value1\", \"valueField2\": \"value2\", "
                        + "\"valueField3\": \"value3\"}");
    }

    public static Object[][] testPrimitiveSchemaTypes() {
        TimeZone.setDefault(TimeZone.getTimeZone(ZoneOffset.UTC));
        return new Object[][] {
            {
                "test",
                TransformSchemaType.BYTES,
                Schema.BYTES,
                "test".getBytes(StandardCharsets.UTF_8)
            },
            {"true", TransformSchemaType.BOOLEAN, Schema.BOOL, true},
            {"42", TransformSchemaType.INT8, Schema.INT8, (byte) 42},
            {"42", TransformSchemaType.INT32, Schema.INT32, 42},
            {"42", TransformSchemaType.INT64, Schema.INT64, 42L},
            {"42.8", TransformSchemaType.FLOAT, Schema.FLOAT, 42.8F},
            {"42.8", TransformSchemaType.DOUBLE, Schema.DOUBLE, 42.8D},
            {
                "2023-01-02T22:04:05.000000006-01:00",
                TransformSchemaType.DATE,
                Schema.DATE,
                new Date(1672700645000L)
            },
            {
                "2023-01-02T22:04:05.000000006-01:00",
                TransformSchemaType.TIMESTAMP,
                Schema.TIMESTAMP,
                Timestamp.from(Instant.ofEpochSecond(1672700645L, 6))
            },
            {"23:04:05.000000006", TransformSchemaType.TIME, Schema.TIME, new Time(83045000L)},
            {
                "2023-01-02T23:04:05.000000006",
                TransformSchemaType.LOCAL_DATE_TIME,
                Schema.LOCAL_DATE_TIME,
                LocalDateTime.of(2023, 1, 2, 23, 4, 5, 6)
            },
            {
                "2023-01-02T22:04:05.000000006-01:00",
                TransformSchemaType.INSTANT,
                Schema.INSTANT,
                Instant.ofEpochSecond(1672700645, 6)
            },
            {
                "2023-01-02",
                TransformSchemaType.LOCAL_DATE,
                Schema.LOCAL_DATE,
                LocalDate.of(2023, 1, 2)
            },
            {
                "23:04:05.000000006",
                TransformSchemaType.LOCAL_TIME,
                Schema.LOCAL_TIME,
                LocalTime.of(23, 4, 5, 6)
            },
        };
    }

    @ParameterizedTest
    @MethodSource("testPrimitiveSchemaTypes")
    void testPrimitiveSchemaTypes(
            String input,
            TransformSchemaType outputSchemaType,
            Schema<?> expectedSchema,
            Object expectedOutput)
            throws Exception {
        Record<GenericObject> record =
                Utils.TestRecord.<GenericObject>builder()
                        .schema(Schema.STRING)
                        .value(
                                AutoConsumeSchema.wrapPrimitiveObject(
                                        input, SchemaType.STRING, new byte[] {}))
                        .build();
        CastStep step = CastStep.builder().valueSchemaType(outputSchemaType).build();
        Record<?> outputRecord = Utils.process(record, step);

        assertEquals(outputRecord.getSchema(), expectedSchema);

        if (expectedOutput instanceof byte[]) {
            assertArrayEquals((byte[]) outputRecord.getValue(), (byte[]) expectedOutput);
        } else {
            assertEquals(outputRecord.getValue(), expectedOutput);
        }
    }
}
