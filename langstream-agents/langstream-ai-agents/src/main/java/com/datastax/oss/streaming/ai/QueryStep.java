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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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

    public static final String MODE_QUERY = "query";
    public static final String MODE_EXECUTE = "execute";

    @Builder.Default private final List<String> fields;
    private final String outputFieldName;
    private final String mode = MODE_QUERY;
    private final String query;
    private final List<String> generatedKeys;
    private final boolean onlyFirst;
    private final QueryStepDataSource dataSource;
    private final Map<Schema, Schema> avroValueSchemaCache = new ConcurrentHashMap<>();
    private final Map<Schema, Schema> avroKeySchemaCache = new ConcurrentHashMap<>();
    private final List<JstlEvaluator<Object>> fieldsEvaluators = new ArrayList<>();
    private final String loopOver;
    private JstlEvaluator<List> loopOverAccessor;

    QueryStep(
            List<String> fields,
            String outputFieldName,
            String query,
            boolean onlyFirst,
            QueryStepDataSource dataSource,
            String loopOver,
            JstlEvaluator<List> loopOverAccessor,
            List<String> generatedKeys) {
        this.fields = fields;
        this.generatedKeys = generatedKeys;
        this.outputFieldName = outputFieldName;
        this.query = query;
        this.onlyFirst = onlyFirst;
        this.dataSource = dataSource;
        this.loopOver = loopOver;
        this.loopOverAccessor = loopOverAccessor;
        this.fields.forEach(
                field -> {
                    fieldsEvaluators.add(new JstlEvaluator<>("${" + field + "}", Object.class));
                });
        if (loopOver != null && !loopOver.isEmpty()) {
            this.loopOverAccessor = new JstlEvaluator<List>("${" + loopOver + "}", List.class);
        } else {
            this.loopOverAccessor = null;
        }
    }

    @Override
    public void process(TransformContext transformContext) {
        List<Map<String, Object>> results;
        if (loopOverAccessor == null) {
            results = performQuery(transformContext);
        } else {
            // loop over a list
            // for each item we name if "record" and we perform the query
            List<Object> nestedRecords = loopOverAccessor.evaluate(transformContext);
            results = new ArrayList<>();
            for (Object document : nestedRecords) {
                TransformContext nestedRecordContext = new TransformContext();
                nestedRecordContext.setRecordObject(document);
                // set in the item the result
                List<Map<String, Object>> resultsForDocument = performQuery(nestedRecordContext);
                results.addAll(resultsForDocument);
            }
        }

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
    }

    private List<Map<String, Object>> performQuery(TransformContext transformContext) {
        List<Object> params = new ArrayList<>();
        fieldsEvaluators.forEach(
                field -> {
                    if (log.isDebugEnabled()) {
                        log.debug("Evaluating {}", field);
                    }
                    Object value = field.evaluate(transformContext);
                    if (log.isDebugEnabled()) {
                        log.debug(
                                "Result {} type {}",
                                value,
                                value != null ? value.getClass() : "null");
                    }
                    params.add(value);
                });
        List<Map<String, Object>> results = dataSource.fetchData(query, params);
        if (results == null) {
            results = List.of();
        }
        if (log.isDebugEnabled()) {
            log.debug("Result from datasource: {}", results);
        }
        return results;
    }

    private Map<String, Object> executeStatement(TransformContext transformContext) {
        List<Object> params = new ArrayList<>();
        fieldsEvaluators.forEach(
                field -> {
                    if (log.isDebugEnabled()) {
                        log.debug("Evaluating {}", field);
                    }
                    Object value = field.evaluate(transformContext);
                    if (log.isDebugEnabled()) {
                        log.debug(
                                "Result {} type {}",
                                value,
                                value != null ? value.getClass() : "null");
                    }
                    params.add(value);
                });
        Map<String, Object> results = dataSource.executeStatement(query, generatedKeys, params);
        if (log.isDebugEnabled()) {
            log.debug("Result from datasource: {}", results);
        }
        return results;
    }
}
