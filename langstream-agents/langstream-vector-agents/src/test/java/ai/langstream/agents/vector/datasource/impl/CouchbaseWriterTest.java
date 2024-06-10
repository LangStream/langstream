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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.langstream.agents.vector.VectorDBSinkAgent;
import ai.langstream.agents.vector.couchbase.CouchbaseDataSource;
import ai.langstream.api.runner.code.AgentCodeRegistry;
import ai.langstream.api.runner.code.AgentContext;
import ai.langstream.api.runner.code.MetricsReporter;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SimpleRecord;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.couchbase.BucketDefinition;
import org.testcontainers.couchbase.CouchbaseContainer;
import org.testcontainers.couchbase.CouchbaseService;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@Disabled
@Testcontainers
class CouchbaseWriterTest {

    BucketDefinition bucketDefinition = new BucketDefinition("bucket-name");

    @Container
    public static CouchbaseContainer couchbaseContainer =
            new CouchbaseContainer("couchbase/server:7.6.1")
                    .withBucket(new BucketDefinition("testbucket").withPrimaryIndex(true))
                    .withStartupTimeout(Duration.ofMinutes(5))
                    .withEnabledServices(
                            CouchbaseService.KV,
                            CouchbaseService.INDEX,
                            CouchbaseService.QUERY,
                            CouchbaseService.SEARCH)
                    .waitingFor(
                            new HttpWaitStrategy()
                                    .forPort(8091)
                                    .forPath("/pools/default/b/testbucket")
                                    .forStatusCodeMatching(
                                            response -> response == 200 || response == 401)
                                    .withStartupTimeout(Duration.ofMinutes(5)));

    private static void createVectorSearchIndex() throws IOException {
        String bucketName = "testbucket";
        String scopeName = "_default";
        String ftsIndexName = "semantic";
        String collectionName = "_default";
        String fullIndexName = bucketName + "." + scopeName + "." + ftsIndexName;
        String indexDefinition =
                "{\n"
                        + "  \"type\": \"fulltext-index\",\n"
                        + "  \"name\": \""
                        + fullIndexName
                        + "\",\n"
                        + "  \"sourceType\": \"gocbcore\",\n"
                        + "  \"sourceName\": \"testbucket\",\n"
                        + "  \"planParams\": {\"maxPartitionsPerPIndex\": 512},\n"
                        + "  \"params\": {\n"
                        + "    \"doc_config\": {\n"
                        + "      \"mode\": \"scope.collection.type_field\",\n"
                        + "      \"type_field\": \"type\"\n"
                        + "    },\n"
                        + "    \"mapping\": {\n"
                        + "      \"analysis\": {},\n"
                        + "      \"default_analyzer\": \"standard\",\n"
                        + "      \"default_datetime_parser\": \"dateTimeOptional\",\n"
                        + "      \"default_field\": \"_all\",\n"
                        + "      \"default_mapping\": {\"dynamic\": false, \"enabled\": false},\n"
                        + "      \"types\": {\n"
                        + "        \""
                        + scopeName
                        + "."
                        + collectionName
                        + "\": {\n"
                        + "          \"dynamic\": false,\n"
                        + "          \"enabled\": true,\n"
                        + "          \"properties\": {\n"
                        + "            \"embeddings\": {\n"
                        + "              \"fields\": [{\"dims\": 1536, \"index\": true, \"name\": \"embeddings\", \"similarity\": \"dot_product\", \"type\": \"vector\"}]\n"
                        + "            },\n"
                        + "            \"vecPlanId\": {\n"
                        + "              \"fields\": [{\"index\": true, \"store\": true, \"name\": \"vecPlanId\", \"type\": \"text\"}]\n"
                        + "            }\n"
                        + "          }\n"
                        + "        }\n"
                        + "      }\n"
                        + "    }\n"
                        + "  }\n"
                        + "}";

        String username = couchbaseContainer.getUsername();
        String password = couchbaseContainer.getPassword();
        String host = couchbaseContainer.getHost();
        int port = couchbaseContainer.getMappedPort(8094);

        URL url =
                new URL(
                        "http://"
                                + host
                                + ":"
                                + port
                                + "/api/bucket/"
                                + bucketName
                                + "/scope/"
                                + scopeName
                                + "/index/"
                                + ftsIndexName);
        HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
        httpConn.setDoOutput(true);
        httpConn.setRequestMethod("PUT");
        httpConn.setRequestProperty("Content-Type", "application/json");
        httpConn.setRequestProperty(
                "Authorization",
                "Basic "
                        + Base64.getEncoder()
                                .encodeToString(
                                        (username + ":" + password)
                                                .getBytes(StandardCharsets.UTF_8)));
        httpConn.getOutputStream().write(indexDefinition.getBytes(StandardCharsets.UTF_8));
        httpConn.getOutputStream().flush();
        httpConn.getOutputStream().close();

        int responseCode = httpConn.getResponseCode();
        if (responseCode != 200) {
            InputStream errorStream = httpConn.getErrorStream();
            String errorMessage =
                    new BufferedReader(new InputStreamReader(errorStream))
                            .lines()
                            .collect(Collectors.joining("\n"));
            throw new IOException(
                    "Failed to create index: HTTP response code "
                            + responseCode
                            + ", message: "
                            + errorMessage);
        }
    }

    private static void listAndLogIndexes() throws IOException {
        String username = couchbaseContainer.getUsername();
        String password = couchbaseContainer.getPassword();
        String host = couchbaseContainer.getHost();
        int port = couchbaseContainer.getMappedPort(8094);

        URL url = new URL("http://" + host + ":" + port + "/api/index");
        HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
        httpConn.setRequestMethod("GET");
        httpConn.setRequestProperty(
                "Authorization",
                "Basic "
                        + Base64.getEncoder()
                                .encodeToString(
                                        (username + ":" + password)
                                                .getBytes(StandardCharsets.UTF_8)));

        int responseCode = httpConn.getResponseCode();
        if (responseCode == 200) {
            InputStream inputStream = httpConn.getInputStream();
            String response =
                    new BufferedReader(new InputStreamReader(inputStream))
                            .lines()
                            .collect(Collectors.joining("\n"));
            System.out.println("Indexes: " + response);
        } else {
            InputStream errorStream = httpConn.getErrorStream();
            String errorMessage =
                    new BufferedReader(new InputStreamReader(errorStream))
                            .lines()
                            .collect(Collectors.joining("\n"));
            throw new IOException(
                    "Failed to list indexes: HTTP response code "
                            + responseCode
                            + ", message: "
                            + errorMessage);
        }
    }

    @Test
    void testCouchbaseWrite() throws Exception {

        Map<String, Object> datasourceConfig =
                Map.of(
                        "service", "couchbase",
                        "connection-string", couchbaseContainer.getConnectionString(),
                        "username", couchbaseContainer.getUsername(),
                        "password", couchbaseContainer.getPassword());

        VectorDBSinkAgent agent =
                (VectorDBSinkAgent)
                        new AgentCodeRegistry().getAgentCode("vector-db-sink").agentCode();

        Map<String, Object> configuration = new HashMap<>();
        configuration.put("datasource", datasourceConfig);
        configuration.put("vector.id", "value.id");
        configuration.put("vector.vector", "value.embeddings");
        configuration.put("bucket-name", "value.bucket");
        configuration.put("scope-name", "value.scope");
        configuration.put("collection-name", "value.collection");

        AgentContext agentContext = mock(AgentContext.class);
        when(agentContext.getMetricsReporter()).thenReturn(MetricsReporter.DISABLED);
        agent.init(configuration);
        agent.start();
        agent.setContext(agentContext);

        List<Record> committed = new CopyOnWriteArrayList<>();
        List<Double> vector = new ArrayList<>();
        for (int i = 1; i <= 1536; i++) {
            vector.add(1.0 / i);
        }
        for (int i = 0; i < 10; i++) {
            String vecPlanId = (i < 5) ? "12345" : "67890";
            String id = "test-doc" + (i + 1);
            Map<String, Object> value =
                    Map.of(
                            "id",
                            id,
                            "document",
                            "Hello " + (i + 1),
                            "embeddings",
                            vector,
                            "vecPlanId",
                            vecPlanId,
                            "bucket",
                            "testbucket",
                            "scope",
                            "_default",
                            "collection",
                            "_default");
            SimpleRecord record =
                    SimpleRecord.of(null, new ObjectMapper().writeValueAsString(value));
            agent.write(record).thenRun(() -> committed.add(record)).get();
        }

        assertEquals(10, committed.size());
        agent.close();

        createVectorSearchIndex();

        listAndLogIndexes();

        // Sleep for a while to allow the data to be indexed
        log.info("Sleeping for 5 seconds to allow the data to be indexed");
        Thread.sleep(5000);

        CouchbaseDataSource dataSource = new CouchbaseDataSource();
        QueryStepDataSource implementation =
                dataSource.createDataSourceImplementation(datasourceConfig);
        implementation.initialize(null);

        String query =
                """
                {
                      "vector": ?,
                      "topK": 5,
                      "bucket-name": "testbucket",
                      "vecPlanId": "12345",
                      "scope-name": "_default",
                      "collection-name": "_default",
                      "vector-name":"semantic",
                      "semantic-name":"semantic"
                    }
                """;
        List<Object> params = List.of(vector);
        List<Map<String, Object>> results = implementation.fetchData(query, params);
        log.info("Results: {}", results);

        for (Map<String, Object> result : results) {
            assertEquals("12345", result.get("vecPlanId"));
        }

        assertEquals(5, results.size());

        // Test that the results contain the correct ids
        for (Map<String, Object> result : results) {
            assertEquals("12345", result.get("vecPlanId")); // Check vecPlanId matches
            assertNotNull(result.get("id")); // Check id is not null
            assertNotNull(result.get("document")); // Check document is not null
            assertNotNull(result.get("bucket"));
            assertNotNull(result.get("scope"));
            assertNotNull(result.get("collection"));
            assertNotNull(result.get("similarity"));
            // assert similarity is between 0.0 and 1.0
            //     assertTrue((double) result.get("similarity") >= 0.0);
            //     assertTrue((double) result.get("similarity") <= 1.0);
        }
    }
}
