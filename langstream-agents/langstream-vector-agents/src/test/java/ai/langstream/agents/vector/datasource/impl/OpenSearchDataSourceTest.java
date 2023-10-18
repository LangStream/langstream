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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.TestCase.assertTrue;

import ai.langstream.agents.vector.opensearch.OpenSearchAssetsManagerProvider;
import ai.langstream.agents.vector.opensearch.OpenSearchWriter;
import ai.langstream.api.model.AssetDefinition;
import ai.langstream.api.runner.assets.AssetManager;
import ai.langstream.api.runner.code.SimpleRecord;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.opensearch.testcontainers.OpensearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Slf4j
@Testcontainers
class OpenSearchDataSourceTest {

    @Container
    static OpensearchContainer OPENSEARCH =
            new OpensearchContainer(DockerImageName.parse("opensearchproject/opensearch:2"))
                    .withEnv("discovery.type", "single-node");

    private static Map<String, Object> getDatasourceConfig(String indexName) {
        return Map.of(
                "https",
                false,
                "port",
                OPENSEARCH.getMappedPort(9200),
                "host",
                "localhost",
                "username",
                "admin",
                "password",
                "admin",
                "index-name",
                indexName);
    }

    @Test
    void testAsset() throws Exception {
        final Map<String, Object> assetConfig =
                Map.of(
                        "settings",
                        """
                       {
                          "index": {
                            "number_of_shards": 2,
                            "number_of_replicas": 1
                          }
                        }
                                              """,
                        "mappings",
                        """
                        {
                            "properties": {
                              "age": {
                                "type": "integer"
                              }
                            }
                        }
                        """,
                        "datasource",
                        Map.of("configuration", getDatasourceConfig("test-index")));
        final AssetManager instance = createAssetManager(assetConfig);

        instance.deployAsset();
        assertTrue(instance.assetExists());
        instance.deleteAssetIfExists();
    }

    private AssetManager createAssetManager(Map<String, Object> config) throws Exception {
        final AssetManager instance =
                new OpenSearchAssetsManagerProvider().createInstance("opensearch-index");
        final AssetDefinition def = new AssetDefinition();
        def.setConfig(config);
        instance.initialize(def);
        return instance;
    }

    @Test
    void testWriteBasicData() throws Exception {
        final String indexName = "test-index-000";
        final Map<String, Object> assetConfig =
                Map.of("datasource", Map.of("configuration", getDatasourceConfig(indexName)));
        createAssetManager(assetConfig).deleteAssetIfExists();
        createAssetManager(assetConfig).deployAsset();
        try (final OpenSearchWriter.OpenSearchVectorDatabaseWriter writer =
                new OpenSearchWriter().createImplementation(getDatasourceConfig(indexName)); ) {

            writer.initialise(
                    Map.of(
                            "index-name",
                            indexName,
                            "fields",
                            List.of(
                                    Map.of("name", "text1", "expression", "value.text"),
                                    Map.of("name", "chunk_id", "expression", "key.chunk_id")),
                            "id",
                            "key.name",
                            "bulk-parameters",
                            Map.of("refresh", "true")));
            SimpleRecord record =
                    SimpleRecord.of(
                            "{\"name\": \"{\\\"myid\\\":\\\"xx\\\"}\", \"chunk_id\": 1}",
                            """
                                    {
                                        "text": "Lorem ipsum..."
                                    }
                                    """);

            writer.upsert(record, Map.of()).get();

            List<Map<String, Object>> result =
                    writer.getDataSource()
                            .fetchData(
                                    """
                            {
                                "index": "test-index-000",
                                "query": {
                                    "match": {
                                        "text1": "Lorem ipsum..."
                                    }

                                }
                            }
                            """,
                                    List.of());
            log.info("result: {}", result);

            assertEquals(1, result.size());
            final String id = "{\"myid\":\"xx\"}";
            assertEquals(id, result.get(0).get("id"));
            assertEquals(1, ((Map) result.get(0).get("document")).get("chunk_id"));
            assertEquals("test-index-000", result.get(0).get("index"));
            assertTrue(((Number) result.get(0).get("score")).floatValue() > 0.0f);

            result =
                    writer.getDataSource()
                            .fetchData(
                                    """
                    {
                        "index": "test-index-000",
                        "query": {
                            "ids": {
                                "values": [?]
                            }

                        }
                    }
                    """,
                                    List.of(id));
            assertEquals(1, result.size());
            writer.upsert(
                            SimpleRecord.of(
                                    "{\"name\": \"{\\\"myid\\\":\\\"xx\\\"}\", \"chunk_id\": 1}",
                                    null),
                            Map.of())
                    .get();

            result =
                    writer.getDataSource()
                            .fetchData(
                                    """
                    {
                        "index": "test-index-000",
                        "query": {
                            "ids": {
                                "values": [?]
                            }

                        }
                    }
                    """,
                                    List.of(id));

            assertEquals(0, result.size());
        }
    }

    @Test
    void testWriteVectors() throws Exception {
        final String indexName = "test-index-000";
        final Map<String, Object> assetConfig =
                Map.of(
                        "settings",
                        """
                        {
                            "index": {
                                  "knn": true,
                                  "knn.algo_param.ef_search": 100
                            }
                        }
                        """,
                        "mappings",
                        """
                                {
                                    "properties": {
                                      "my_vector1": {
                                        "type": "knn_vector",
                                        "dimension": 3,
                                        "method": {
                                          "name": "hnsw",
                                          "space_type": "l2",
                                          "engine": "lucene",
                                          "parameters": {
                                            "ef_construction": 128,
                                            "m": 24
                                          }
                                        }
                                      }
                                    }
                                }

                        """,
                        "datasource",
                        Map.of("configuration", getDatasourceConfig(indexName)));
        createAssetManager(assetConfig).deleteAssetIfExists();
        createAssetManager(assetConfig).deployAsset();
        try (final OpenSearchWriter.OpenSearchVectorDatabaseWriter writer =
                new OpenSearchWriter().createImplementation(getDatasourceConfig(indexName)); ) {

            writer.initialise(
                    Map.of(
                            "index-name",
                            indexName,
                            "fields",
                            List.of(
                                    Map.of("name", "my_vector1", "expression", "value.embeddings"),
                                    Map.of("name", "original", "expression", "value.data")),
                            "bulk-parameters",
                            Map.of("refresh", "true")));
            SimpleRecord record =
                    SimpleRecord.of(
                            null,
                            """
                                    {
                                        "data": "This is a question",
                                        "embeddings": [1.0, 2.0, 3.0]
                                    }
                                    """);

            writer.upsert(record, Map.of()).get();

            final List<Map<String, Object>> result =
                    writer.getDataSource()
                            .fetchData(
                                    """
                            {
                                "size": 2,
                                "index": "test-index-000",
                                "query": {
                                    "knn": {
                                      "my_vector1": {
                                        "vector": [26.2, -120.0, 99.1],
                                        "k": 2
                                      }
                                    }
                                  }
                            }
                            """,
                                    List.of());
            log.info("result: {}", result);

            assertEquals(1, result.size());
            assertNotNull(result.get(0).get("id"));
            assertEquals(
                    "This is a question", ((Map) result.get(0).get("document")).get("original"));
            assertEquals(
                    List.of(1.0d, 2.0d, 3.0d),
                    ((Map) result.get(0).get("document")).get("my_vector1"));
            assertEquals("test-index-000", result.get(0).get("index"));
        }
    }
}
