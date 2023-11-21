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
import io.stargate.sdk.json.domain.Filter;
import io.stargate.sdk.json.domain.JsonResult;
import io.stargate.sdk.json.domain.SelectQuery;
import io.stargate.sdk.json.domain.SelectQueryBuilder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AstraVectorDBDataSource implements QueryStepDataSource {

    AstraDB astraDB;

    @Override
    public void initialize(Map<String, Object> dataSourceConfig) {
        log.info(
                "Initializing CassandraDataSource with config {}",
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
        Map<String, Object> queryMap =
                InterpolationUtils.buildObjectFromJson(query, Map.class, params);
        if (queryMap.isEmpty()) {
            throw new UnsupportedOperationException("Query is empty");
        }
        String collectionName = (String) queryMap.get("collection-name");
        if (collectionName == null) {
            throw new UnsupportedOperationException("collection-name is not defined");
        }
        CollectionClient collection = this.getAstraDB().collection(collectionName);
        List<JsonResult> result;

        float[] vector = JstlFunctions.toArrayOfFloat(queryMap.remove("vector"));
        Integer max = (Integer) queryMap.remove("max");

        if (max == null) {
            max = Integer.MAX_VALUE;
        }
        if (vector != null) {
            Filter filter = new Filter();
            queryMap.forEach((k, v) -> filter.where(k).isEqualsTo(v));
            log.info(
                    "doing similarity search with filter {} max {} and vector {}",
                    filter,
                    max,
                    vector);
            result = collection.similaritySearch(vector, filter, max);
        } else {
            SelectQueryBuilder selectQueryBuilder =
                    SelectQuery.builder().includeSimilarity().select("*");
            queryMap.forEach((k, v) -> selectQueryBuilder.where(k).isEqualsTo(v));

            SelectQuery selectQuery = selectQueryBuilder.build();
            log.info("doing query {}", selectQuery);

            result = collection.query(selectQuery).toList();
        }

        return result.stream()
                .map(
                        m -> {
                            Map<String, Object> r = new HashMap<>();
                            if (m.getData() != null) {
                                r.putAll(m.getData());
                            }
                            if (m.getSimilarity() != null) {
                                r.put("similarity", m.getSimilarity());
                            }
                            if (m.getVector() != null) {
                                r.put("vector", JstlFunctions.toListOfFloat(m.getVector()));
                            }
                            return r;
                        })
                .collect(Collectors.toList());
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
        throw new UnsupportedOperationException();
    }

    public AstraDB getAstraDB() {
        return astraDB;
    }
}
