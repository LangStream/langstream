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
package com.datastax.oss.streaming.ai.jstl.predicate;

import static com.datastax.oss.streaming.ai.Utils.newTransformContext;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.langstream.ai.agents.commons.MutableRecord;
import ai.langstream.ai.agents.commons.jstl.predicate.JstlPredicate;
import com.datastax.oss.streaming.ai.Utils;
import java.util.HashMap;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.functions.api.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class JstlPredicateTest {

    @ParameterizedTest
    @MethodSource("keyValuePredicates")
    void testKeyValueAvro(String when, boolean match) {
        JstlPredicate predicate = new JstlPredicate(when);

        Record<GenericObject> record = Utils.createNestedAvroKeyValueRecord(2);
        Utils.TestContext context = new Utils.TestContext(record, new HashMap<>());
        MutableRecord mutableRecord =
                newTransformContext(context, record.getValue().getNativeObject());

        assertEquals(predicate.test(mutableRecord), match);
    }

    @Test
    void testInvalidWhen() {
        assertEquals(
                "invalid when: `invalid",
                assertThrows(
                                IllegalArgumentException.class,
                                () -> {
                                    new JstlPredicate("`invalid");
                                })
                        .getMessage());
    }

    @ParameterizedTest
    @MethodSource("primitiveKeyValuePredicates")
    void testPrimitiveKeyValueAvro(String when, MutableRecord context, boolean match) {
        JstlPredicate predicate = new JstlPredicate(when);
        assertEquals(predicate.test(context), match);
    }

    @ParameterizedTest
    @MethodSource("primitivePredicates")
    void testPrimitiveValueAvro(String when, MutableRecord context, boolean match) {
        JstlPredicate predicate = new JstlPredicate(when);
        assertEquals(predicate.test(context), match);
    }

    /**
     * @return {"expression", "transform context" "expected match boolean"}
     */
    public static Object[][] primitiveKeyValuePredicates() {
        Schema<KeyValue<String, Integer>> keyValueSchema =
                Schema.KeyValue(Schema.STRING, Schema.INT32, KeyValueEncodingType.SEPARATED);

        KeyValue<String, Integer> keyValue = new KeyValue<>("key", 42);

        MutableRecord primitiveKVContext =
                Utils.createContextWithPrimitiveRecord(keyValueSchema, keyValue, "");

        return new Object[][] {
            // match
            {"key=='key' && value==42", primitiveKVContext, true},
            // no-match
            {"key=='key' && value<42", primitiveKVContext, false},
        };
    }

    /**
     * @return {"expression", "transform context" "expected match boolean"}
     */
    public static Object[][] primitivePredicates() {
        Schema<KeyValue<String, Integer>> keyValueSchema =
                Schema.KeyValue(Schema.STRING, Schema.INT32, KeyValueEncodingType.SEPARATED);
        KeyValue<String, Integer> keyValue = new KeyValue<>("key", 42);

        MutableRecord primitiveStringContext =
                Utils.createContextWithPrimitiveRecord(Schema.STRING, "test-message", "header-key");
        MutableRecord primitiveIntContext =
                Utils.createContextWithPrimitiveRecord(Schema.INT32, 33, "header-key");
        MutableRecord primitiveKVContext =
                Utils.createContextWithPrimitiveRecord(keyValueSchema, keyValue, "header-key");

        return new Object[][] {
            // match
            {"value=='test-message'", primitiveStringContext, true},
            {"messageKey=='header-key'", primitiveStringContext, true},
            {"key=='header-key'", primitiveStringContext, true},
            {"value==33", primitiveIntContext, true},
            {"value eq 33", primitiveIntContext, true},
            {"value eq 32 + 1", primitiveIntContext, true},
            {"value eq 34 - 1", primitiveIntContext, true},
            {"value eq 66 / 2", primitiveIntContext, true},
            {"value eq 66 div 2", primitiveIntContext, true},
            {"value % 10 == 3", primitiveIntContext, true},
            {"value mod 10 == 3", primitiveIntContext, true},
            {"value>32", primitiveIntContext, true},
            {"value gt 32", primitiveIntContext, true},
            {"value<=33 && key=='header-key'", primitiveIntContext, true},
            {"key=='key' && value==42", primitiveKVContext, true},
            {"key=='key' and value==42", primitiveKVContext, true},
            {"key=='key1' || value==42", primitiveKVContext, true},
            {"key=='key1' or value==42", primitiveKVContext, true},
            {"key=='key' && value==42", primitiveKVContext, true},
            // no-match
            {"value=='test-message-'", primitiveStringContext, false},
            {"key!='header-key'", primitiveStringContext, false},
            {"key ne 'header-key'", primitiveStringContext, false},
            {"value==34", primitiveIntContext, false},
            {"value>33", primitiveIntContext, false},
            {"value<=20 && key=='test-key'", primitiveIntContext, false},
            {"value le 20 && key=='test-key'", primitiveIntContext, false},
        };
    }

    /**
     * @return {"expression", "expected match boolean"}
     */
    public static Object[][] keyValuePredicates() {
        return new Object[][] {
            // match
            {"key.level1String == 'level1_1'", true},
            {"key.level1Record.level2String == 'level2_1'", true},
            {"key.level1Record.level2Integer == 9", true},
            {"key.level1Record.level2Double == 8.8", true},
            {"key.level1Record.level2Array[0] == 'level2_1'", true},
            {"value.level1Record.level2Integer > 8", true},
            {"value.level1Record.level2Double < 8.9", true},
            {"value.level1Record.level2Array[0] == 'level2_1'", true},
            {"messageKey == 'key1'", true},
            {"destinationTopic == 'dest-topic-1'", true},
            {"topicName == 'topic-1'", true},
            {"properties.p1 == 'v1'", true},
            {"properties.p2 == 'v2'", true},
            // no match
            {"key.level1String == 'leVel1_1'", false},
            {"key.level1Record.random == 'level2_1'", false},
            {"key.level1Record.level2Integer != 9", false},
            {"key.level1Record.level2Double < 8.8", false},
            {"key.level1Record.level2Array[0] == 'non_existing_item'", false},
            {"key.randomKey == 'k1'", false},
            {"value.level1Record.level2Integer > 10", false},
            {"value.level1Record.level2Double < 0", false},
            {"value.randomValue < 0", false},
            {"messageKey == 'key2'", false},
            {"topicName != 'topic-1'", false},
            {"properties.p2 == 'v3'", false},
            {"randomHeader == 'h1'", false}
        };
    }
}
