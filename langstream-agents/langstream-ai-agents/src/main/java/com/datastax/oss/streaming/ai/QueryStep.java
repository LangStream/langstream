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
    private final Map<Schema, Schema> avroValueSchemaCache = new ConcurrentHashMap<>();
    private final Map<Schema, Schema> avroKeySchemaCache = new ConcurrentHashMap<>();
    private final List<JstlEvaluator<Object>> fieldsEvaluators = new ArrayList<>();

    @Builder.Default private final List<String> fields;
    private final String outputFieldName;
    private final String query;
    private final boolean onlyFirst;
    private final QueryStepDataSource dataSource;
    private final String loopOver;
    private final List<String> generatedKeys;
    private final String mode;

    private JstlEvaluator<List> loopOverAccessor;

    QueryStep(
            List<String> fields,
            String outputFieldName,
            String query,
            boolean onlyFirst,
            QueryStepDataSource dataSource,
            String loopOver,
            List<String> generatedKeys,
            String mode,
            JstlEvaluator<List> loopOverAccessor) {
        this.fields = fields;
        this.mode = mode == null ? MODE_QUERY : mode;
        this.generatedKeys = generatedKeys;
        this.outputFieldName = outputFieldName;
        this.query = query;
        this.onlyFirst = onlyFirst;
        this.dataSource = dataSource;
        this.loopOver = loopOver;
        this.loopOverAccessor = loopOverAccessor;
        if (this.fields != null) {
            this.fields.forEach(
                    field -> {
                        fieldsEvaluators.add(new JstlEvaluator<>("${" + field + "}", Object.class));
                    });
        }
        if (loopOver != null && !loopOver.isEmpty()) {
            this.loopOverAccessor = new JstlEvaluator<>("${" + loopOver + "}", List.class);
        } else {
            this.loopOverAccessor = null;
        }
    }

    @Override
    public void process(MutableRecord mutableRecord) {
        Schema schema;
        Object finalResult;

        switch (mode) {
            case MODE_QUERY:
                List<Map<String, Object>> results = processQuery(mutableRecord);
                if (onlyFirst) {
                    schema = Schema.createMap(Schema.create(Schema.Type.STRING));
                    if (results.isEmpty()) {
                        finalResult = Map.of();
                    } else {
                        finalResult = results.get(0);
                    }
                } else {
                    schema =
                            Schema.createArray(Schema.createMap(Schema.create(Schema.Type.STRING)));
                    finalResult = results;
                }
                break;
            case MODE_EXECUTE:
                finalResult = processExecute(mutableRecord);
                if (finalResult instanceof Map) {
                    schema = Schema.createMap(Schema.create(Schema.Type.STRING));
                } else if (finalResult instanceof List) {
                    schema =
                            Schema.createArray(Schema.createMap(Schema.create(Schema.Type.STRING)));
                } else {
                    throw new IllegalStateException();
                }
                break;
            default:
                throw new IllegalStateException("Unknown mode " + mode);
        }
        mutableRecord.setResultField(
                finalResult, outputFieldName, schema, avroKeySchemaCache, avroValueSchemaCache);
    }

    private List<Map<String, Object>> processQuery(MutableRecord mutableRecord) {
        List<Map<String, Object>> results;
        if (loopOverAccessor == null) {
            results = performQuery(mutableRecord);
        } else {
            // loop over a list
            // for each item we name if "record" and we perform the query
            List<Object> nestedRecords = loopOverAccessor.evaluate(mutableRecord);
            if (nestedRecords == null) {
                log.info("Property {} not found in record {}", loopOver, mutableRecord);
                nestedRecords = List.of();
            }
            results = new ArrayList<>();
            for (Object document : nestedRecords) {
                MutableRecord nestedRecordContext = new MutableRecord();
                nestedRecordContext.setRecordObject(document);
                // set in the item the result
                List<Map<String, Object>> resultsForDocument = performQuery(nestedRecordContext);
                results.addAll(resultsForDocument);
            }
        }
        return results;
    }

    private Object processExecute(MutableRecord mutableRecord) {
        Object res;
        if (loopOverAccessor == null) {
            res = executeStatement(mutableRecord);
        } else {
            // loop over a list
            // for each item we name if "record" and we perform the query
            List<Object> nestedRecords = loopOverAccessor.evaluate(mutableRecord);
            if (nestedRecords == null) {
                log.info("Property {} not found in record {}", loopOver, mutableRecord);
                nestedRecords = List.of();
            }
            List<Map<String, Object>> results = new ArrayList<>();
            for (Object document : nestedRecords) {
                MutableRecord nestedRecordContext = new MutableRecord();
                nestedRecordContext.setRecordObject(document);
                // set in the item the result
                Map<String, Object> resultsForDocument = executeStatement(nestedRecordContext);
                results.add(resultsForDocument);
            }
            res = results;
        }
        return res;
    }

    private List<Map<String, Object>> performQuery(MutableRecord mutableRecord) {
        List<Object> params = new ArrayList<>();
        fieldsEvaluators.forEach(
                field -> {
                    if (log.isDebugEnabled()) {
                        log.debug("Evaluating {}", field);
                    }
                    Object value = field.evaluate(mutableRecord);
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

    private Map<String, Object> executeStatement(MutableRecord mutableRecord) {
        List<Object> params = new ArrayList<>();
        fieldsEvaluators.forEach(
                field -> {
                    if (log.isDebugEnabled()) {
                        log.debug("Evaluating {}", field);
                    }
                    Object value = field.evaluate(mutableRecord);
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
