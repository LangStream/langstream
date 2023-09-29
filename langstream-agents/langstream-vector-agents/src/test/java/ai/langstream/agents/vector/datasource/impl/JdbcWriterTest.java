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

import ai.langstream.agents.vector.jdbc.JdbcAssetsManagerProvider;
import ai.langstream.agents.vector.jdbc.JdbcWriter;
import ai.langstream.ai.agents.datasource.impl.JdbcDataSourceProvider;
import ai.langstream.api.model.AssetDefinition;
import ai.langstream.api.runner.assets.AssetManager;
import ai.langstream.api.runner.assets.AssetManagerProvider;
import ai.langstream.api.runner.code.SimpleRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class JdbcWriterTest {

    static final String CREATE_TABLE =
            "CREATE TABLE DOCUMENTS (\n"
                    + "  NAME string,\n"
                    + "  CHUNK_ID int,  \n"
                    + "  TEXT string,  \n"
                    + "  vector floata, \n"
                    + "  PRIMARY KEY(NAME, CHUNK_ID) \n"
                    + ")";

    static final String DROP_TABLE = "DROP TABLE DOCUMENTS";

    @Test
    void testWrite() throws Exception {
        JdbcDataSourceProvider dataSourceProvider = new JdbcDataSourceProvider();
        Map<String, Object> config =
                Map.of(
                        "url",
                        "jdbc:herddb:local",
                        "driverClass",
                        herddb.jdbc.Driver.class.getName());

        String tableName = "documents";
        int dimension = 32;
        List<Float> vector = new ArrayList<>();
        List<Float> vector2 = new ArrayList<>();
        for (int i = 0; i < dimension; i++) {
            vector.add(i * 1f / dimension);
            vector2.add((i + 1) * 1f / dimension);
        }
        String vectorAsString = vector.toString();
        String vector2AsString = vector2.toString();

        try (JdbcDataSourceProvider.JdbcDataSourceImpl datasource =
                        dataSourceProvider.createDataSourceImplementation(config);
                JdbcWriter.JdbcVectorDatabaseWriter writer =
                        new JdbcWriter().createImplementation(config)) {
            datasource.initialize(null);

            AssetManagerProvider assetsManagerProvider = new JdbcAssetsManagerProvider();
            try (AssetManager tableManager = assetsManagerProvider.createInstance("jdbc-table"); ) {
                AssetDefinition assetDefinition = new AssetDefinition();
                assetDefinition.setAssetType("jdbc-table");
                assetDefinition.setConfig(
                        Map.of(
                                "table-name",
                                tableName,
                                "datasource",
                                Map.of("configuration", config),
                                "create-statements",
                                List.of(CREATE_TABLE),
                                "delete-statements",
                                List.of(DROP_TABLE)));
                tableManager.initialize(assetDefinition);
                tableManager.deleteAssetIfExists();

                assertFalse(tableManager.assetExists());
                tableManager.deployAsset();

                List<Map<String, Object>> fields =
                        List.of(
                                Map.of(
                                        "name",
                                        "name",
                                        "expression",
                                        "key.name",
                                        "primary-key",
                                        true),
                                Map.of(
                                        "name",
                                        "chunk_id",
                                        "expression",
                                        "key.chunk_id",
                                        "primary-key",
                                        true),
                                Map.of(
                                        "name",
                                        "vector",
                                        "expression",
                                        "fn:toListOfFloat(value.vector)"),
                                Map.of("name", "text", "expression", "value.text"));

                writer.initialise(Map.of("table-name", tableName, "fields", fields));

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
                        "SELECT name,text from documents order by cosine_similarity(vector,cast(? as float array)) DESC";
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
