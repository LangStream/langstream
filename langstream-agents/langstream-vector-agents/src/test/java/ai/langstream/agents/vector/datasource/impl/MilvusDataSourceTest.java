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
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Slf4j
@Testcontainers
class MilvusDataSourceTest {

    @Container
    private GenericContainer milvus =
            new GenericContainer(new DockerImageName("langstream/milvus-lite:latest-dev"))
                    .withExposedPorts(19530)
                    .withLogConsumer(new Slf4jLogConsumer(log).withPrefix("milvus"))
                    .withStartupTimeout(java.time.Duration.ofMinutes(5)) // on M1 it is slower
                    .waitingFor(new LogMessageWaitStrategy().withRegEx(".*http.*"));

    @Test
    void testMilvusQuery() throws Exception {

        String collectionName = "book";
        String databaseName = "default";

        int milvusPort = milvus.getMappedPort(19530);
        String milvusHost = milvus.getHost();
        log.info("Connecting to Milvus at {}:{}", milvusHost, milvusPort);

        MilvusDataSource dataSource = new MilvusDataSource();
        Map<String, Object> config =
                Map.of(
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
                        List.of(
                                """
                        {
                            "collection-name": "%s",
                            "database-name": "%s",
                            "dimension": 5,
                            "field-types": [
                               {
                                  "name": "id",
                                  "primary-key": true,
                                  "data-type": "Int64"
                               },
                               {
                                  "name": "text",
                                  "data-type": "string"
                               }
                            ]
                        }
                        """
                                        .formatted(collectionName, databaseName))));
        collectionManager.initialize(assetDefinition);
        collectionManager.deleteAssetIfExists();
        assertFalse(collectionManager.assetExists());
        collectionManager.deployAsset();

        try (QueryStepDataSource datasource = dataSource.createDataSourceImplementation(config);
                MilvusWriter.MilvusVectorDatabaseWriter writer =
                        new MilvusWriter().createImplementation(config)) {

            datasource.initialize(null);

            writer.initialise(
                    Map.of(
                            "collection-name",
                            collectionName,
                            "fields",
                            List.of(
                                    Map.of(
                                            "name",
                                            "vector",
                                            "expression",
                                            "fn:toListOfFloat(value.vector)"),
                                    Map.of("name", "text", "expression", "value.text"),
                                    Map.of("name", "id", "expression", "fn:toLong(key.id)"))));

            SimpleRecord record =
                    SimpleRecord.of(
                            "{\"id\": 10}",
                            """
                    {
                        "vector": [1,2,3,4,5],
                        "text": "Lorem ipsum..."
                    }
                    """);
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
                                  "output-fields": ["id", "distance", "text"]
                            }
                            """
                            .formatted(collectionName, databaseName);
            List<Object> params = List.of(List.of(1f, 2f, 3f, 4f, 5f));
            List<Map<String, String>> results = datasource.fetchData(query, params);
            log.info("Results: {}", results);

            assertEquals(1, results.size());
            assertEquals("10", results.get(0).get("id"));
            assertEquals("0.0", results.get(0).get("distance"));
            assertEquals("Lorem ipsum...", results.get(0).get("text"));
        }
    }
}
