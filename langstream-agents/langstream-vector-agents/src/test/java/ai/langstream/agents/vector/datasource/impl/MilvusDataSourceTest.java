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

import static org.junit.jupiter.api.Assertions.assertFalse;

import ai.langstream.agents.vector.milvus.MilvusAssetsManagerProvider;
import ai.langstream.agents.vector.milvus.MilvusDataSource;
import ai.langstream.api.model.AssetDefinition;
import ai.langstream.api.runner.assets.AssetManager;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import java.io.File;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@Testcontainers
class MilvusDataSourceTest {

    @Container
    public DockerComposeContainer<?> environment =
            new DockerComposeContainer<>(new File("src/test/resources/milvus-compose-test.yml"))
                    .withExposedService("milvus", 19530)
                    .withLogConsumer("milvus", new Slf4jLogConsumer(log).withPrefix("milvus"));

    @Test
    void testMilvusQuery() throws Exception {

        String collectionName = "book";
        String databaseName = "default";

        int milvusPort = 19530;
        String milvusHost = "localhost";
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
                            "out-fields": []
                        }
                        """
                                        .formatted(collectionName, databaseName))));
        collectionManager.initialize(assetDefinition);
        collectionManager.deleteAssetIfExists();
        assertFalse(collectionManager.assetExists());
        collectionManager.deployAsset();

        try (QueryStepDataSource implementation =
                dataSource.createDataSourceImplementation(config); ) {
            implementation.initialize(null);
            String query =
                    """
                            {
                                  "vectors": ?,
                                  "topK": 5,
                                  "vector-field-name": "vector",
                                  "collection-name": "%s",
                                  "database-name": "%s"
                            }
                            """
                            .formatted(collectionName, databaseName);
            List<Object> params = List.of(List.of(0.1f, 0.1f, 0.1f, 0.1f, 0.1f));
            List<Map<String, String>> results = implementation.fetchData(query, params);
            log.info("Results: {}", results);
        }
    }
}
