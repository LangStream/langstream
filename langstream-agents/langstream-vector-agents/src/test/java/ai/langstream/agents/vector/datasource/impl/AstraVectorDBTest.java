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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
@Slf4j
public class AstraVectorDBTest {

    private static final String TOKEN =
            "AstraCS:HQKZyFwTNcNQFPhsLHPHlyYq:0fd08e29b7e7c590e947ac8fa9a4d6d785a4661a8eb1b3c011e2a0d19c2ecd7c";
    private static final String ENDPOINT =
            "https://18bdf302-901f-4245-af09-061ebdb480d2-us-east1.apps.astra.datastax.com";

    @Test
    void testWriteAndRead() throws Exception {
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

                // INSERT

                String name = "do'c1";

                // the PK contains a single quote in order to test escaping values in deletion
                SimpleRecord record =
                        SimpleRecord.of(
                                "{\"name\": \"%s\", \"chunk_id\": 1}".formatted(name),
                                """
                                        {
                                            "vector": %s,
                                            "text": "Lorem ipsum..."
                                        }
                                        """
                                        .formatted(vectorAsString));
                writer.upsert(record, Map.of()).get();

                String similarityQuery =
                        """
                        {
                            "collection-name": "%s",
                            "vector": ?,
                            "max": 10
                        }
                        """
                                .formatted(collectionName);

                // QUERY, SIMILARITY SEARCH
                assertContents(
                        datasource,
                        similarityQuery,
                        List.of(vector),
                        results -> {
                            assertEquals(1, results.size());
                            assertEquals(name, results.get(0).get("name"));
                            assertEquals("Lorem ipsum...", results.get(0).get("text"));
                        });

                // QUERY, SIMILARITY SEARCH WITH METADATA FILTERING
                String similarityQueryWithFilterOnName =
                        """
                        {
                            "collection-name": "%s",
                            "vector": ?,
                            "max": 10,
                            "filter": {
                                "name": ?
                            }
                        }
                        """
                                .formatted(collectionName);
                assertContents(
                        datasource,
                        similarityQueryWithFilterOnName,
                        List.of(vector, name),
                        results -> {
                            assertEquals(1, results.size());
                            assertEquals(name, results.get(0).get("name"));
                            assertEquals("Lorem ipsum...", results.get(0).get("text"));
                        });

                assertContents(
                        datasource,
                        """
                        {
                            "collection-name": "%s",
                            "vector": ?,
                            "max": 10,
                            "filter": {
                                "name": ?
                            },
                            "select": ["text"],
                            "include-similarity": true
                        }
                        """
                                .formatted(collectionName),
                        List.of(vector, name),
                        results -> {
                            assertEquals(1, results.size());
                            assertNull(results.get(0).get("name"));
                            assertEquals("Lorem ipsum...", results.get(0).get("text"));
                            assertNotNull(results.get(0).get("similarity"));
                            assertNull(results.get(0).get("vector"));
                        });

                assertContents(
                        datasource,
                        """
                        {
                            "collection-name": "%s",
                            "vector": ?,
                            "max": 10,
                            "filter": {
                                "name": ?
                            },
                            "select": ["text"],
                            "include-similarity": false
                        }
                        """
                                .formatted(collectionName),
                        List.of(vector, name),
                        results -> {
                            assertEquals(1, results.size());
                            assertNull(results.get(0).get("name"));
                            assertEquals("Lorem ipsum...", results.get(0).get("text"));
                            assertNull(results.get(0).get("similarity"));
                            assertNull(results.get(0).get("vector"));
                        });

                assertContents(
                        datasource,
                        """
                        {
                            "collection-name": "%s",
                            "vector": ?,
                            "max": 10,
                            "filter": {
                                "name": ?
                            },
                            "select": ["text", "$vector"],
                            "include-similarity": false
                        }
                        """
                                .formatted(collectionName),
                        List.of(vector, name),
                        results -> {
                            assertEquals(1, results.size());
                            assertNull(results.get(0).get("name"));
                            assertEquals("Lorem ipsum...", results.get(0).get("text"));
                            assertNull(results.get(0).get("similarity"));
                            assertNotNull(results.get(0).get("vector"));
                        });

                String queryWithFilterOnName =
                        """
                        {
                            "collection-name": "%s",
                            "max": 10,
                            "filter": {
                                "name": ?
                            }
                        }
                        """
                                .formatted(collectionName);

                // QUERY, WITH METADATA FILTERING
                assertContents(
                        datasource,
                        queryWithFilterOnName,
                        List.of(name),
                        results -> {
                            assertEquals(1, results.size());
                        });

                // QUERY, WITH METADATA FILTERING, NO RESULTS
                assertContents(
                        datasource,
                        queryWithFilterOnName,
                        List.of("bad-name"),
                        results -> {
                            assertEquals(0, results.size());
                        });

                // UPDATE
                SimpleRecord recordUpdated =
                        SimpleRecord.of(
                                "{\"name\": \"%s\", \"chunk_id\": 1}".formatted(name),
                                """
                                        {
                                            "vector": %s,
                                            "text": "Lorem ipsum changed..."
                                        }
                                        """
                                        .formatted(vector2AsString));
                writer.upsert(recordUpdated, Map.of()).get();

                assertContents(
                        datasource,
                        similarityQuery,
                        List.of(vector2),
                        results -> {
                            log.info("Results: {}", results);
                            assertEquals(1, results.size());
                            assertEquals(name, results.get(0).get("name"));
                            assertEquals("Lorem ipsum changed...", results.get(0).get("text"));
                        });

                // DELETE
                SimpleRecord recordDelete =
                        SimpleRecord.of(
                                "{\"name\": \"%s\", \"chunk_id\": 1}".formatted(name), null);
                writer.upsert(recordDelete, Map.of()).get();

                assertContents(
                        datasource,
                        similarityQuery,
                        List.of(vector2),
                        result -> {
                            assertEquals(0, result.size());
                        });

                Map<String, Object> executeInsertRes =
                        executeStatement(
                                datasource,
                                """
                                {
                                    "action": "insertOne",
                                    "collection-name": "%s",
                                    "document": {
                                        "id": "some-id",
                                        "name": ?,
                                        "vector": ?,
                                        "text": "Some text"
                                    }
                                }
                                """
                                        .formatted(collectionName),
                                List.of("some", vector));
                assertEquals("some-id", executeInsertRes.get("id"));

                assertContents(
                        datasource,
                        queryWithFilterOnName,
                        List.of("some"),
                        result -> {
                            assertEquals(1, result.size());
                            assertEquals("Some text", result.get(0).get("text"));
                        });

                executeStatement(
                        datasource,
                        """
                        {
                            "action": "findOneAndUpdate",
                            "collection-name": "%s",
                            "filter": {
                                "_id": ?
                            },
                            "update": {
                                "$set": {
                                    "text": ?
                                }
                            }
                        }
                        """
                                .formatted(collectionName),
                        List.of("some-id", "new value"));

                assertContents(
                        datasource,
                        queryWithFilterOnName,
                        List.of("some"),
                        result -> {
                            assertEquals(1, result.size());
                            assertEquals("new value", result.get(0).get("text"));
                        });

                executeStatement(
                        datasource,
                        """
                        {
                            "action": "deleteOne",
                            "collection-name": "%s",
                            "filter": {
                                "_id": ?
                            }
                        }
                        """
                                .formatted(collectionName),
                        List.of("some-id"));

                assertContents(
                        datasource,
                        queryWithFilterOnName,
                        List.of("some"),
                        result -> {
                            assertEquals(0, result.size());
                        });

                // CLEANUP
                assertTrue(tableManager.assetExists());
                tableManager.deleteAssetIfExists();
            }
        }
    }

    private static List<Map<String, Object>> assertContents(
            QueryStepDataSource datasource,
            String query,
            List<Object> params,
            Consumer<List<Map<String, Object>>> assertion) {
        log.info("Query: {}", query);
        log.info("Params: {}", params);
        List<Map<String, Object>> results = datasource.fetchData(query, params);
        log.info("Result: {}", results);
        assertion.accept(results);
        ;
        return results;
    }

    private static Map<String, Object> executeStatement(
            QueryStepDataSource datasource, String query, List<Object> params) {
        log.info("Query: {}", query);
        log.info("Params: {}", params);
        Map<String, Object> results = datasource.executeStatement(query, null, params);
        log.info("Result: {}", results);
        return results;
    }
}
