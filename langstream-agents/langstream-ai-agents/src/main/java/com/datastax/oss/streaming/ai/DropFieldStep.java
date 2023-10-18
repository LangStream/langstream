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

import ai.langstream.ai.agents.commons.MutableRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Builder;

/** This function removes a "field" from a message. */
@Builder
public class DropFieldStep implements TransformStep {

    @Builder.Default private final List<String> keyFields = new ArrayList<>();
    @Builder.Default private final List<String> valueFields = new ArrayList<>();

    private final Map<org.apache.avro.Schema, org.apache.avro.Schema> keySchemaCache =
            new ConcurrentHashMap<>();
    private final Map<org.apache.avro.Schema, org.apache.avro.Schema> valueSchemaCache =
            new ConcurrentHashMap<>();

    @Override
    public void process(MutableRecord mutableRecord) {
        if (mutableRecord.getKeyObject() != null) {
            mutableRecord.dropKeyFields(keyFields, keySchemaCache);
        }
        mutableRecord.dropValueFields(valueFields, valueSchemaCache);
    }
}
