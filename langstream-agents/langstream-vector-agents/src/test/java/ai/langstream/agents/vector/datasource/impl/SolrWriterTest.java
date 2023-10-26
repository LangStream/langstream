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

import ai.langstream.agents.vector.solr.SolrAssetsManagerProvider;
import ai.langstream.agents.vector.solr.SolrDataSource;
import ai.langstream.agents.vector.solr.SolrWriter;
import ai.langstream.api.model.AssetDefinition;
import ai.langstream.api.runner.assets.AssetManager;
import ai.langstream.api.runner.code.SimpleRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@Testcontainers
public class SolrWriterTest {

    static final int DIMENSIONS = 1356;

    @Container
    private GenericContainer solrCloudContainer =
            new GenericContainer("solr:9.3.0")
                    .withExposedPorts(8983)
                    .withCommand("-c"); // enable SolarCloud mode

    static final Map<String, Object> CREATE_COLLECTION =
            Map.of(
                    "api",
                    "/api/collections",
                    "body",
                    """
                    {
                      "name": "documents",
                      "numShards": 1,
                      "replicationFactor": 1
                    }
                    """);

    static final Map<String, Object> ADD_FIELD_TYPE =
            Map.of(
                    "api",
                    "/schema",
                    "body",
                    """
                    "add-field-type" : {
                         "name": "knn_vector",
                         "class": "solr.DenseVectorField",
                         "vectorDimension": "%s",
                         "similarityFunction": "cosine"
                         }
                    """
                            .formatted(DIMENSIONS));
    static final Map<String, Object> ADD_FIELD_EMBEDDINGS =
            Map.of(
                    "api",
                    "/schema",
                    "body",
                    """
                    "add-field":{
                         "name":"embeddings",
                         "type":"knn_vector",
                         "stored":true,
                         "indexed":true
                         }
                    """);

    static final Map<String, Object> ADD_FIELD_NAME =
            Map.of(
                    "api",
                    "/schema",
                    "body",
                    """
                    "add-field":{
                         "name":"name",
                         "type":"string",
                         "stored":true,
                         "indexed":false,
                         "multiValued": false
                         }
                    """);

    static final Map<String, Object> ADD_FIELD_CHUNK_ID =
            Map.of(
                    "api",
                    "/schema",
                    "body",
                    """
                    "add-field":{
                         "name":"chunk_id",
                         "type":"string",
                         "stored":true,
                         "indexed":false,
                         "multiValued": false
                         }
                    """);

    static final Map<String, Object> ADD_FIELD_TEXT =
            Map.of(
                    "api",
                    "/schema",
                    "body",
                    """
                    "add-field":{
                         "name":"text",
                         "type":"string",
                         "stored":true,
                         "indexed":false,
                         "multiValued": false
                         }
                    """);

    @Test
    void testWrite() throws Exception {
        Map<String, Object> config =
                Map.of(
                        "service",
                        "solr",
                        "host",
                        "localhost",
                        "port",
                        solrCloudContainer.getMappedPort(8983),
                        "collection-name",
                        "documents");
        SolrDataSource dataSourceProvider = new SolrDataSource();

        String collectoinName = "documents";

        List<Float> vector = new ArrayList<>();
        List<Float> vector2 = new ArrayList<>();
        for (int i = 0; i < DIMENSIONS; i++) {
            vector.add(i * 1f / DIMENSIONS);
            vector2.add((i + 1) * 1f / DIMENSIONS);
        }
        String vectorAsString = vector.toString();
        String vector2AsString = vector2.toString();

        try (SolrDataSource.SolrQueryStepDataSource datasource =
                        dataSourceProvider.createDataSourceImplementation(config);
                SolrWriter.SolrVectorDatabaseWriter writer =
                        new SolrWriter().createImplementation(config)) {
            datasource.initialize(null);

            SolrAssetsManagerProvider assetsManagerProvider = new SolrAssetsManagerProvider();
            try (AssetManager tableManager =
                    assetsManagerProvider.createInstance("solr-collection"); ) {
                AssetDefinition assetDefinition = new AssetDefinition();
                assetDefinition.setAssetType("solr-collection");
                assetDefinition.setConfig(
                        Map.of(
                                "datasource",
                                Map.of("configuration", config),
                                "create-statements",
                                List.of(
                                        CREATE_COLLECTION,
                                        ADD_FIELD_TYPE,
                                        ADD_FIELD_EMBEDDINGS,
                                        ADD_FIELD_NAME,
                                        ADD_FIELD_TEXT,
                                        ADD_FIELD_CHUNK_ID)));
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
                                        "fn:concat3(key.name, '-', fn:toString(key.chunk_id))",
                                        "primary-key",
                                        true),
                                Map.of("name", "name", "expression", "key.name"),
                                Map.of("name", "chunk_id", "expression", "key.chunk_id"),
                                Map.of(
                                        "name",
                                        "embeddings",
                                        "expression",
                                        "fn:toListOfFloat(value.vector)"),
                                Map.of("name", "text", "expression", "value.text"));

                writer.initialise(
                        Map.of("table-name", collectoinName, "fields", fields, "commit-within", 0));

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
                                "q": "{!knn f=embeddings topK=10}?"
                                }
                                """; // the ? will be replaced by the vector
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
                assertEquals("1", results2.get(0).get("chunk_id"));

                SimpleRecord recordDelete =
                        SimpleRecord.of("{\"name\": \"do'c1\", \"chunk_id\": 1}", null);
                writer.upsert(recordDelete, Map.of()).get();

                List<Map<String, Object>> results3 = datasource.fetchData(query, params2);
                log.info("Results: {}", results3);
                assertEquals(0, results3.size());

                assertTrue(tableManager.assetExists());
                assertTrue(tableManager.deleteAssetIfExists());
                assertFalse(tableManager.assetExists());
                assertFalse(tableManager.deleteAssetIfExists());
            }
        }
    }
}
