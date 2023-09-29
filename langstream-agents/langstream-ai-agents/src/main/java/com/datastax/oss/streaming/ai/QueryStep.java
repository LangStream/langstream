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

import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import com.datastax.oss.streaming.ai.model.TransformSchemaType;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

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

    @Override
    public void process(TransformContext transformContext) {
        List<Object> params = new ArrayList<>();
        fields.forEach(
                field -> {
                    if (field.equals("value")) {
                        params.add(transformContext.getValueObject());
                    } else if (field.equals("key")) {
                        params.add(transformContext.getKeyObject());
                    } else if (field.equals("messageKey")) {
                        params.add(transformContext.getKey());
                    } else if (field.startsWith("properties.")) {
                        String propName = field.substring("properties.".length());
                        params.add(transformContext.getProperties().get(propName));
                    } else if (field.equals("destinationTopic")) {
                        params.add(transformContext.getOutputTopic());
                    } else if (field.equals("topicName")) {
                        params.add(transformContext.getInputTopic());
                    } else if (field.equals("eventTime")) {
                        params.add(transformContext.getEventTime());
                    } else if (field.startsWith("value.")) {
                        params.add(
                                getField(
                                        "value",
                                        field,
                                        transformContext.getValueSchemaType(),
                                        transformContext.getValueObject()));
                    } else if (field.startsWith("key.")) {
                        params.add(
                                getField(
                                        "key",
                                        field,
                                        transformContext.getKeySchemaType(),
                                        transformContext.getKeyObject()));
                    }
                });

        List<Map<String, Object>> results = dataSource.fetchData(query, params);
        if (results == null) {
            results = List.of();
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

    private Object getField(
            String key, String field, TransformSchemaType keySchemaType, Object keyObject) {
        String fieldName = field.substring((key.length() + 1));
        if (keyObject instanceof Map) {
            return ((Map<String, Object>) keyObject).get(fieldName);
        }
        switch (keySchemaType) {
            case AVRO:
                GenericRecord avroRecord = (GenericRecord) keyObject;
                return getAvroField(fieldName, avroRecord);
            case JSON:
                JsonNode json = (JsonNode) keyObject;
                return getJsonField(fieldName, json);
            default:
                throw new TransformFunctionException(
                        String.format(
                                "%s.* can only be used in query step with AVRO or JSON schema",
                                key));
        }
    }

    private static Object getJsonField(String fieldName, JsonNode json) {
        JsonNode node = json.get(fieldName);
        if (node != null && !node.isNull()) {
            if (node.isArray()) {
                List<Object> values = new ArrayList<>();
                for (JsonNode elem : node) {
                    values.add(jsonNodeToPrimitive(elem));
                }
                return values;
            } else {
                return jsonNodeToPrimitive(node);
            }
        } else {
            throw new TransformFunctionException(
                    String.format("Field %s is null in JSON record", fieldName));
        }
    }

    private static Object jsonNodeToPrimitive(JsonNode node) {
        if (node.isNumber()) {
            return node.asDouble();
        } else if (node.isBoolean()) {
            return node.asBoolean();
        } else {
            return node.asText();
        }
    }

    private static Object getAvroField(String fieldName, GenericRecord avroRecord) {
        Object rawValue = avroRecord.get(fieldName);
        if (rawValue != null) {
            // TODO: handle numbers...
            if (rawValue instanceof CharSequence) {
                // AVRO utf8...
                rawValue = rawValue.toString();
            }
        } else {
            throw new TransformFunctionException(
                    String.format("Field %s is null in AVRO record", fieldName));
        }
        return rawValue;
    }
}
