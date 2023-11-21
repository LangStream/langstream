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
package ai.langstream.agents.vector.datasource.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.langstream.agents.vector.astra.AstraVectorDBAssetsManagerProvider;
import ai.langstream.agents.vector.astra.AstraVectorDBDataSourceProvider;
import ai.langstream.agents.vector.astra.AstraVectorDBWriter;
import ai.langstream.api.database.VectorDatabaseWriter;
import ai.langstream.api.model.AssetDefinition;
import ai.langstream.api.runner.assets.AssetManager;
import ai.langstream.api.runner.assets.AssetManagerProvider;
import ai.langstream.api.runner.code.SimpleRecord;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class AstraVectorDBTest {

    private static final String TOKEN =
            "AstraCS:";
    private static final String ENDPOINT =
            "https://";

    @Test
    void testWrite() throws Exception {
        AstraVectorDBDataSourceProvider dataSourceProvider = new AstraVectorDBDataSourceProvider();
        Map<String, Object> config = Map.of("token", TOKEN, "endpoint", ENDPOINT);

        String collectionName = "documents";
        int dimension = 32;
        List<Float> vector = new ArrayList<>();
        List<Float> vector2 = new ArrayList<>();
        for (int i = 0; i < dimension; i++) {
            vector.add(i * 1f / dimension);
            vector2.add((i + 1) * 1f / dimension);
        }
        String vectorAsString = vector.toString();
        String vector2AsString = vector2.toString();

        try (QueryStepDataSource datasource =
                        dataSourceProvider.createDataSourceImplementation(config);
                VectorDatabaseWriter writer =
                        new AstraVectorDBWriter().createImplementation(config)) {
            datasource.initialize(config);

            AssetManagerProvider assetsManagerProvider = new AstraVectorDBAssetsManagerProvider();
            try (AssetManager tableManager =
                    assetsManagerProvider.createInstance("astra-collection"); ) {
                AssetDefinition assetDefinition = new AssetDefinition();
                assetDefinition.setAssetType("astra-collection");
                assetDefinition.setConfig(
                        Map.of(
                                "collection-name",
                                collectionName,
                                "datasource",
                                Map.of("configuration", config),
                                "vector-dimension",
                                vector.size()));
                tableManager.initialize(assetDefinition);
                tableManager.deleteAssetIfExists();

                assertFalse(tableManager.assetExists());
                tableManager.deployAsset();

                List<Map<String, Object>> fields =
                        List.of(
                                Map.of(
                                        "name",
                                        "id",
                                        "expression",
                                        "fn:concat(key.name,'-',key.chunk_id)"),
                                Map.of("name", "name", "expression", "key.name"),
                                Map.of("name", "chunk_id", "expression", "key.chunk_id"),
                                Map.of(
                                        "name",
                                        "vector",
                                        "expression",
                                        "fn:toListOfFloat(value.vector)"),
                                Map.of("name", "text", "expression", "value.text"));

                writer.initialise(Map.of("collection-name", collectionName, "fields", fields));

                // the PK contains a single quote in order to test escaping values in deletion
                SimpleRecord record =
                        SimpleRecord.of(
                                "{\"name\": \"do'c1\", \"chunk_id\": 1}",
                                """
                                        {
                                            "vector": %s,
                                            "text": "Lorem ipsum..."
                                        }
                                        """
                                        .formatted(vectorAsString));
                writer.upsert(record, Map.of()).get();

                String query =
                        """
                        {
                            "collection-name": "%s",
                            "vector": ?,
                            "max": 10
                        }
                        """
                                .formatted(collectionName);
                List<Object> params = List.of(vector);
                List<Map<String, Object>> results = datasource.fetchData(query, params);
                log.info("Results: {}", results);

                assertEquals(1, results.size());
                assertEquals("do'c1", results.get(0).get("name"));
                assertEquals("Lorem ipsum...", results.get(0).get("text"));

                SimpleRecord recordUpdated =
                        SimpleRecord.of(
                                "{\"name\": \"do'c1\", \"chunk_id\": 1}",
                                """
                                        {
                                            "vector": %s,
                                            "text": "Lorem ipsum changed..."
                                        }
                                        """
                                        .formatted(vector2AsString));
                writer.upsert(recordUpdated, Map.of()).get();

                List<Object> params2 = List.of(vector2);
                List<Map<String, Object>> results2 = datasource.fetchData(query, params2);
                log.info("Results: {}", results2);

                assertEquals(1, results2.size());
                assertEquals("do'c1", results2.get(0).get("name"));
                assertEquals("Lorem ipsum changed...", results2.get(0).get("text"));

                SimpleRecord recordDelete =
                        SimpleRecord.of("{\"name\": \"do'c1\", \"chunk_id\": 1}", null);
                writer.upsert(recordDelete, Map.of()).get();

                List<Map<String, Object>> results3 = datasource.fetchData(query, params2);
                log.info("Results: {}", results3);
                assertEquals(0, results3.size());

                assertTrue(tableManager.assetExists());
                tableManager.deleteAssetIfExists();
            }
        }
    }
}
