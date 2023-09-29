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

import ai.langstream.agents.vector.milvus.MilvusAssetsManagerProvider;
import ai.langstream.agents.vector.milvus.MilvusDataSource;
import ai.langstream.agents.vector.milvus.MilvusWriter;
import ai.langstream.api.model.AssetDefinition;
import ai.langstream.api.runner.assets.AssetManager;
import ai.langstream.api.runner.code.SimpleRecord;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@Testcontainers
class MilvusDataSourceTest {

    // @Container
    @ParameterizedTest
    @Disabled("Milvus is not available in the CI environment")
    @ValueSource(booleans = {true, false})
    void testMilvusQuery(boolean useCreateSimpleCollection) throws Exception {
        int dimension = 32; // minimum for Zillis

        int milvusPort = 443;

        boolean usingZillisService = false;
        String milvusHost = "localhost";
        String writeMode = usingZillisService ? "delete-insert" : "upsert";
        String url = "";
        String token = "";

        String collectionName = "coll" + UUID.randomUUID().toString().replace("-", "");
        String databaseName = "";

        List<String> createCollectionStatements =
                List.of(
                        """
                        {
                            "command": "create-collection",
                            "collection-name": "%s",
                            "database-name": "%s",
                            "field-types": [
                               {
                                  "name": "name",
                                  "primary-key": true,
                                  "data-type": "Varchar",
                                  "max-length": 256
                               },
                               {
                                  "name": "text",
                                  "data-type": "Varchar",
                                  "max-length": 256
                               },
                               {
                                  "name": "vector",
                                  "data-type": "FloatVector",
                                  "dimension": %d
                               }
                            ]
                        }
                        """
                                .formatted(collectionName, databaseName, dimension),
                        """
                                   {
                                       "command": "create-index",
                                       "collection-name": "%s",
                                       "database-name": "%s",
                                       "field-name": "vector",
                                       "index-name": "vector_index",
                                       "index-type": "AUTOINDEX",
                                       "metric-type": "L2"
                                   }
                                """
                                .formatted(collectionName, databaseName),
                        """
                           {
                               "command": "load-collection"
                           }
                        """);

        List<String> createSimpleCollectionCommands =
                List.of(
                        """
                        {
                            "command": "create-simple-collection",
                            "collection-name": "%s",
                            "database-name": "%s",
                            "dimension": 32,
                            "auto-id": false,
                            "output-field": ["name", "text"]
                        }
                        """
                                .formatted(collectionName, databaseName));

        log.info("Connecting to Milvus at {}:{}", milvusHost, milvusPort);

        MilvusDataSource dataSource = new MilvusDataSource();
        Map<String, Object> config =
                Map.of(
                        "write-mode",
                        writeMode,
                        "url",
                        url,
                        "token",
                        token,
                        "user",
                        "root",
                        "password",
                        "Milvus",
                        "host",
                        milvusHost,
                        "port",
                        milvusPort);

        MilvusAssetsManagerProvider assetsManagerProvider = new MilvusAssetsManagerProvider();
        AssetManager collectionManager = assetsManagerProvider.createInstance("milvus-collection");
        AssetDefinition assetDefinition = new AssetDefinition();
        assetDefinition.setAssetType("milvus-collection");
        assetDefinition.setConfig(
                Map.of(
                        "collection-name",
                        collectionName,
                        "datasource",
                        Map.of("configuration", config),
                        "database-name",
                        databaseName,
                        "create-statements",
                        useCreateSimpleCollection
                                ? createSimpleCollectionCommands
                                : createCollectionStatements));
        collectionManager.initialize(assetDefinition);
        collectionManager.deleteAssetIfExists();
        assertFalse(collectionManager.assetExists());
        collectionManager.deployAsset();

        List<Float> vector = new ArrayList<>();
        List<Float> vector2 = new ArrayList<>();
        for (int i = 0; i < dimension; i++) {
            vector.add(i * 1f / dimension);
            vector2.add((i + 1) * 1f / dimension);
        }
        String vectorAsString = vector.toString();
        String vector2AsString = vector2.toString();

        try (QueryStepDataSource datasource = dataSource.createDataSourceImplementation(config);
                MilvusWriter.MilvusVectorDatabaseWriter writer =
                        new MilvusWriter().createImplementation(config)) {

            datasource.initialize(null);

            // with a "SimpleCollection" the PK is always "id"
            List<Map<String, Object>> fields =
                    useCreateSimpleCollection
                            ? List.of(
                                    Map.of("name", "id", "expression", "1"),
                                    Map.of(
                                            "name",
                                            "name",
                                            "expression",
                                            "fn:concat(key.name, key.chunk_id)"),
                                    Map.of(
                                            "name",
                                            "vector",
                                            "expression",
                                            "fn:toListOfFloat(value.vector)"),
                                    Map.of("name", "text", "expression", "value.text"))
                            : List.of(
                                    Map.of(
                                            "name",
                                            "name",
                                            "expression",
                                            "fn:concat(key.name, key.chunk_id)"),
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
                                  "vectors": ?,
                                  "topK": 5,
                                  "vector-field-name": "vector",
                                  "collection-name": "%s",
                                  "database-name": "%s",
                                  "consistency-level": "STRONG",
                                  "params": "",
                                  "output-fields": ["name", "text"]
                            }
                            """
                            .formatted(collectionName, databaseName);
            List<Object> params = List.of(vector);
            List<Map<String, Object>> results = datasource.fetchData(query, params);
            log.info("Results: {}", results);

            assertEquals(1, results.size());
            assertEquals("do'c11", results.get(0).get("name"));
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
            assertEquals("do'c11", results2.get(0).get("name"));
            assertEquals("Lorem ipsum changed...", results2.get(0).get("text"));

            SimpleRecord recordDelete =
                    SimpleRecord.of("{\"name\": \"do'c1\", \"chunk_id\": 1}", null);
            writer.upsert(recordDelete, Map.of()).get();

            List<Map<String, Object>> results3 = datasource.fetchData(query, params2);
            log.info("Results: {}", results3);
            assertEquals(0, results3.size());
        }
    }
}
