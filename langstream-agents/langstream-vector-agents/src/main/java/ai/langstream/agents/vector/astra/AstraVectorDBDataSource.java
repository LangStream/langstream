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
package ai.langstream.agents.vector.astra;

import ai.langstream.agents.vector.InterpolationUtils;
import ai.langstream.ai.agents.commons.jstl.JstlFunctions;
import ai.langstream.api.util.ConfigurationUtils;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import com.dtsx.astra.sdk.AstraDB;
import io.stargate.sdk.json.CollectionClient;
import io.stargate.sdk.json.domain.DeleteQuery;
import io.stargate.sdk.json.domain.DeleteQueryBuilder;
import io.stargate.sdk.json.domain.JsonDocument;
import io.stargate.sdk.json.domain.JsonResult;
import io.stargate.sdk.json.domain.JsonResultUpdate;
import io.stargate.sdk.json.domain.SelectQuery;
import io.stargate.sdk.json.domain.SelectQueryBuilder;
import io.stargate.sdk.json.domain.UpdateQuery;
import io.stargate.sdk.json.domain.UpdateQueryBuilder;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AstraVectorDBDataSource implements QueryStepDataSource {

    static final Field update;

    static {
        try {
            update = UpdateQueryBuilder.class.getDeclaredField("update");
            update.setAccessible(true);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    AstraDB astraDB;

    @Override
    public void initialize(Map<String, Object> dataSourceConfig) {
        log.info(
                "Initializing AstraVectorDBDataSource with config {}",
                ConfigurationUtils.redactSecrets(dataSourceConfig));
        String astraToken = ConfigurationUtils.getString("token", "", dataSourceConfig);
        String astraEndpoint = ConfigurationUtils.getString("endpoint", "", dataSourceConfig);
        this.astraDB = new AstraDB(astraToken, astraEndpoint);
    }

    @Override
    public void close() {}

    @Override
    public List<Map<String, Object>> fetchData(String query, List<Object> params) {
        if (log.isDebugEnabled()) {
            log.debug(
                    "Executing query {} with params {} ({})",
                    query,
                    params,
                    params.stream()
                            .map(v -> v == null ? "null" : v.getClass().toString())
                            .collect(Collectors.joining(",")));
        }
        try {
            Map<String, Object> queryMap =
                    InterpolationUtils.buildObjectFromJson(query, Map.class, params);
            if (queryMap.isEmpty()) {
                throw new UnsupportedOperationException("Query is empty");
            }
            String collectionName = (String) queryMap.remove("collection-name");
            if (collectionName == null) {
                throw new UnsupportedOperationException("collection-name is not defined");
            }
            CollectionClient collection = this.getAstraDB().collection(collectionName);
            List<JsonResult> result;

            float[] vector = JstlFunctions.toArrayOfFloat(queryMap.remove("vector"));
            Integer limit = (Integer) queryMap.remove("limit");

            boolean includeSimilarity = vector != null;
            Object includeSimilarityParam = queryMap.remove("include-similarity");
            if (includeSimilarityParam != null) {
                includeSimilarity = Boolean.parseBoolean(includeSimilarityParam.toString());
            }
            Map<String, Object> filterMap = (Map<String, Object>) queryMap.remove("filter");
            SelectQueryBuilder selectQueryBuilder = SelectQuery.builder();
            Object selectClause = queryMap.remove("select");
            if (selectClause != null) {
                if (selectClause instanceof List list) {
                    String[] arrayOfStrings = ((List<String>) list).toArray(new String[0]);
                    selectQueryBuilder.select(arrayOfStrings);
                } else {
                    throw new IllegalArgumentException(
                            "select clause must be a list of strings, but found: " + selectClause);
                }
            }
            if (includeSimilarity) {
                selectQueryBuilder.includeSimilarity();
            }
            if (vector != null) {
                selectQueryBuilder.orderByAnn(vector);
            }
            if (filterMap != null) {
                selectQueryBuilder.withJsonFilter(JstlFunctions.toJson(filterMap));
            }
            if (limit != null) {
                selectQueryBuilder.limit(limit);
            }

            SelectQuery selectQuery = selectQueryBuilder.build();
            if (log.isDebugEnabled()) {
                log.debug("doing query {}", JstlFunctions.toJson(selectQuery));
            }

            result = collection.query(selectQuery).toList();

            return result.stream()
                    .map(
                            m -> {
                                Map<String, Object> r = new HashMap<>();
                                if (m.getData() != null) {
                                    r.putAll(m.getData());
                                }
                                r.put("id", m.getId());
                                if (m.getSimilarity() != null) {
                                    r.put("similarity", m.getSimilarity());
                                }
                                if (m.getVector() != null) {
                                    r.put("vector", JstlFunctions.toListOfFloat(m.getVector()));
                                }
                                return r;
                            })
                    .collect(Collectors.toList());
        } catch (Exception err) {
            throw new RuntimeException(err);
        }
    }

    @Override
    public Map<String, Object> executeStatement(
            String query, List<String> generatedKeys, List<Object> params) {
        if (log.isDebugEnabled()) {
            log.debug(
                    "Executing statement {} with params {} ({})",
                    query,
                    params,
                    params.stream()
                            .map(v -> v == null ? "null" : v.getClass().toString())
                            .collect(Collectors.joining(",")));
        }
        try {
            Map<String, Object> queryMap =
                    InterpolationUtils.buildObjectFromJson(query, Map.class, params);
            if (queryMap.isEmpty()) {
                throw new UnsupportedOperationException("Query is empty");
            }
            String collectionName = (String) queryMap.remove("collection-name");
            if (collectionName == null) {
                throw new UnsupportedOperationException("collection-name is not defined");
            }
            CollectionClient collection = this.getAstraDB().collection(collectionName);

            String action = (String) queryMap.remove("action");
            if (action == null) {
                action = "findOneAndUpdate";
            }

            switch (action) {
                case "findOneAndUpdate":
                    {
                        Map<String, Object> filterMap =
                                (Map<String, Object>) queryMap.remove("filter");
                        UpdateQueryBuilder builder = UpdateQuery.builder();
                        if (filterMap != null) {
                            builder.withJsonFilter(JstlFunctions.toJson(filterMap));
                        }
                        String returnDocument = (String) queryMap.remove("return-document");
                        if (returnDocument != null) {
                            builder.withReturnDocument(
                                    UpdateQueryBuilder.ReturnDocument.valueOf(returnDocument));
                        }
                        Map<String, Object> updateMap =
                                (Map<String, Object>) queryMap.remove("update");
                        if (updateMap == null || updateMap.isEmpty()) {
                            throw new IllegalArgumentException("update map cannot be empty");
                        }
                        update.set(builder, updateMap);

                        UpdateQuery updateQuery = builder.build();
                        if (log.isDebugEnabled()) {
                            log.debug(
                                    "doing findOneAndUpdate with UpdateQuery {}",
                                    JstlFunctions.toJson(updateQuery));
                        }
                        JsonResultUpdate oneAndUpdate = collection.findOneAndUpdate(updateQuery);
                        return Map.of("count", oneAndUpdate.getUpdateStatus().getModifiedCount());
                    }
                case "deleteOne":
                    {
                        Map<String, Object> filterMap =
                                (Map<String, Object>) queryMap.remove("filter");
                        DeleteQueryBuilder builder = DeleteQuery.builder();
                        if (filterMap != null) {
                            builder.withJsonFilter(JstlFunctions.toJson(filterMap));
                        }
                        DeleteQuery delete = builder.build();
                        if (log.isDebugEnabled()) {
                            log.debug(
                                    "doing deleteOne with DeleteQuery {}",
                                    JstlFunctions.toJson(delete));
                        }
                        int count = collection.deleteOne(delete);
                        return Map.of("count", count);
                    }
                case "deleteMany":
                    {
                        Map<String, Object> filterMap =
                                (Map<String, Object>) queryMap.remove("filter");
                        DeleteQueryBuilder builder = DeleteQuery.builder();
                        if (filterMap != null) {
                            builder.withJsonFilter(JstlFunctions.toJson(filterMap));
                        }
                        DeleteQuery delete = builder.build();
                        if (log.isDebugEnabled()) {
                            log.debug(
                                    "doing deleteMany with DeleteQuery {}",
                                    JstlFunctions.toJson(delete));
                        }
                        int count = collection.deleteMany(delete);
                        return Map.of("count", count);
                    }
                case "insertOne":
                    {
                        Map<String, Object> documentData =
                                (Map<String, Object>) queryMap.remove("document");
                        JsonDocument document = new JsonDocument();
                        for (Map.Entry<String, Object> entry : documentData.entrySet()) {
                            String key = entry.getKey();
                            Object value = entry.getValue();
                            switch (key) {
                                case "id":
                                    document.id(value.toString());
                                    break;
                                case "vector":
                                    document.vector(JstlFunctions.toArrayOfFloat(value));
                                    break;
                                case "data":
                                    document.data(value);
                                    break;
                                default:
                                    document.put(key, value);
                                    break;
                            }
                        }
                        if (document.getId() == null) {
                            document.setId(UUID.randomUUID().toString());
                        }

                        if (log.isDebugEnabled()) {
                            log.debug(
                                    "doing insertOne with JsonDocument {}",
                                    JstlFunctions.toJson(document));
                        }
                        String id = collection.insertOne(document);
                        return Map.of("id", id);
                    }
                default:
                    throw new UnsupportedOperationException("Unsupported action: " + action);
            }

        } catch (Exception err) {
            throw new RuntimeException(err);
        }
    }

    public AstraDB getAstraDB() {
        return astraDB;
    }
}
