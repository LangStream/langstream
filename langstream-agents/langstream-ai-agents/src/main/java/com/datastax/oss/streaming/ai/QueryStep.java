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

import ai.langstream.ai.agents.commons.TransformContext;
import ai.langstream.ai.agents.commons.jstl.JstlEvaluator;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;

/**
 * Compute AI Embeddings for one or more records fields and put the value into a new or existing
 * field.
 */
@Builder
@Slf4j
public class QueryStep implements TransformStep {

    @Builder.Default private final List<String> fields = new ArrayList<>();
    private final String outputFieldName;
    private final String query;
    private final boolean onlyFirst;
    private final QueryStepDataSource dataSource;
    private final Map<Schema, Schema> avroValueSchemaCache = new ConcurrentHashMap<>();
    private final Map<Schema, Schema> avroKeySchemaCache = new ConcurrentHashMap<>();
    private final List<JstlEvaluator<Object>> fieldsEvaluators = new ArrayList<>();
    private final String loopOver;
    private JstlEvaluator<List> loopOverAccessor;
    private String fieldInRecord;

    @Override
    public void start() throws Exception {
        fields.forEach(
                field -> {
                    fieldsEvaluators.add(new JstlEvaluator<>(field, Object.class));
                });
        if (loopOver != null && !loopOver.isEmpty()) {
            this.loopOverAccessor = new JstlEvaluator<List>(loopOver, List.class);
            if (!outputFieldName.startsWith("record.")) {
                throw new IllegalArgumentException(
                        "With loop-over the embeddings field but be something like record.xxx");
            }
            this.fieldInRecord = outputFieldName.substring(7);
            if (outputFieldName.contains(".")) {
                throw new IllegalArgumentException(
                        "With loop-over the embeddings field but be something like record.xxx,"
                                + "it cannot be record.xxx.yyy");
            }
        } else {
            this.loopOverAccessor = null;
            this.fieldInRecord = null;
        }
    }

    @Override
    public void process(TransformContext transformContext) {
        if (loopOverAccessor == null) {
            List<Map<String, Object>> results = performQuery(transformContext);
            Object finalResult = results;
            Schema schema;
            if (onlyFirst) {
                schema = Schema.createMap(Schema.create(Schema.Type.STRING));
                if (results.isEmpty()) {
                    finalResult = Map.of();
                } else {
                    finalResult = results.get(0);
                }
            } else {
                schema = Schema.createArray(Schema.createMap(Schema.create(Schema.Type.STRING)));
            }
            transformContext.setResultField(
                    finalResult, outputFieldName, schema, avroKeySchemaCache, avroValueSchemaCache);
        } else {
            // loop over a list
            // for each item we name if "record" and we perform the query
            List<Object> nestedRecords = loopOverAccessor.evaluate(transformContext);
            List<Map<String, Object>> newList = new CopyOnWriteArrayList<>();
            for (Object document : nestedRecords) {
                // this is a mutable map
                Map<String, Object> newMap = new HashMap<>((Map<String, Object>) document);
                newList.add(newMap);
                TransformContext nestedRecordContext = new TransformContext();
                nestedRecordContext.setRecordObject(newMap);

                // set in the item the result
                List<Map<String, Object>> results = performQuery(nestedRecordContext);
                newMap.put(fieldInRecord, results);
            }

            // finally we can override the original list with a new one

            // please note that in AVRO there is no Map<String, Object>, we are reporting
            // Map<String, String>
            // but currently we don't care much as this feature is supposed to work with schema less
            // records
            transformContext.setResultField(
                    newList,
                    loopOver,
                    Schema.createArray(Schema.createMap(Schema.create(Schema.Type.STRING))),
                    avroKeySchemaCache,
                    avroValueSchemaCache);
        }
    }

    private List<Map<String, Object>> performQuery(TransformContext transformContext) {
        List<Object> params = new ArrayList<>();
        fieldsEvaluators.forEach(
                field -> {
                    Object value = field.evaluate(transformContext);
                    params.add(value);
                });
        List<Map<String, Object>> results = dataSource.fetchData(query, params);
        if (results == null) {
            results = List.of();
        }
        return results;
    }
}
