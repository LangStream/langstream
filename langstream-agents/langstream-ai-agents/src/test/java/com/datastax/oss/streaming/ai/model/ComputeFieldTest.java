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
package com.datastax.oss.streaming.ai.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class ComputeFieldTest {

    @Test
    void testInvalidComputeFieldName() {
        assertEquals(
                "Invalid compute field name: newStringField. It should be prefixed with 'key.' or 'value.' or 'properties.' or be one of [key, value, destinationTopic, messageKey]",
                assertThrows(
                                IllegalArgumentException.class,
                                () -> {
                                    ComputeField.builder()
                                            .scopedName("newStringField")
                                            .expression("'Hotaru'")
                                            .type(ComputeFieldType.STRING)
                                            .build();
                                })
                        .getMessage());
    }

    @Test
    void testValidKeyComputeFieldName() {
        ComputeField field =
                ComputeField.builder()
                        .scopedName("key.newStringField")
                        .expression("'Hotaru'")
                        .type(ComputeFieldType.STRING)
                        .build();

        assertEquals("newStringField", field.getName());
        assertEquals("key", field.getScope());
    }

    @Test
    void testValidValueComputeFieldName() {
        ComputeField field =
                ComputeField.builder()
                        .scopedName("value.newStringField")
                        .expression("'Hotaru'")
                        .type(ComputeFieldType.STRING)
                        .build();

        assertEquals("newStringField", field.getName());
        assertEquals("value", field.getScope());
    }

    @Test
    void testValidHeaderComputeFieldName() {
        ComputeField field =
                ComputeField.builder()
                        .scopedName("destinationTopic")
                        .expression("'Hotaru'")
                        .type(ComputeFieldType.STRING)
                        .build();

        assertEquals("destinationTopic", field.getName());
        assertEquals("header", field.getScope());
    }

    @Test
    void testPrimitiveValueComputeFieldName() {
        ComputeField field =
                ComputeField.builder().scopedName("value").type(ComputeFieldType.STRING).build();

        assertEquals("value", field.getName());
        assertEquals("primitive", field.getScope());
    }

    @Test
    void testPrimitiveKeyComputeFieldName() {
        ComputeField field =
                ComputeField.builder().scopedName("key").type(ComputeFieldType.STRING).build();

        assertEquals("key", field.getName());
        assertEquals("primitive", field.getScope());
    }
}
