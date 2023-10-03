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

package com.datastax.oss.streaming.ai.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import ai.langstream.ai.agents.commons.AvroUtil;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;

public class AvroUtilTest {
    private static final MyLogicalType MY_LOGICAL_TYPE = new MyLogicalType();

    @Test
    public void testGetLogicalType() {
        // given
        org.apache.avro.Schema myLogicalTypeSchema =
                MY_LOGICAL_TYPE.addToSchema(
                        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BOOLEAN));

        // when
        LogicalType logicalType = AvroUtil.getLogicalType(myLogicalTypeSchema);

        // then
        assertNotNull(logicalType);
        assertEquals("my-logical-type", logicalType.getName());
    }

    @Test
    public void testGetLogicalTypeUnion() {
        // given
        org.apache.avro.Schema myLogicalTypeSchema =
                MY_LOGICAL_TYPE.addToSchema(
                        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BOOLEAN));
        org.apache.avro.Schema unionSchema =
                SchemaBuilder.unionOf().nullType().and().type(myLogicalTypeSchema).endUnion();

        // when
        LogicalType logicalType = AvroUtil.getLogicalType(unionSchema);

        // then
        assertNotNull(logicalType);
        assertEquals("my-logical-type", logicalType.getName());
    }

    public static class MyLogicalType extends LogicalType {
        MyLogicalType() {
            super("my-logical-type");
        }

        @Override
        public void validate(Schema schema) {
            super.validate(schema);
            if (schema.getType() != Schema.Type.BOOLEAN) {
                throw new IllegalArgumentException(
                        "Logical type 'my-logical-type' should be stored as a boolean");
            }
        }
    }
}
