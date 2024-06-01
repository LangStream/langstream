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
package ai.langstream.agents.vector.couchbase;

import static ai.langstream.ai.agents.commons.MutableRecord.recordToMutableRecord;

import ai.langstream.ai.agents.commons.MutableRecord;
import ai.langstream.ai.agents.commons.jstl.JstlEvaluator;
import ai.langstream.api.database.VectorDatabaseWriter;
import ai.langstream.api.database.VectorDatabaseWriterProvider;
import ai.langstream.api.runner.code.Record;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.UpsertOptions;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CouchbaseWriter implements VectorDatabaseWriterProvider {

    @Override
    public boolean supports(Map<String, Object> dataSourceConfig) {
        return "couchbase".equals(dataSourceConfig.get("service"));
    }

    @Override
    public CouchbaseDatabaseWriter createImplementation(Map<String, Object> datasourceConfig) {
        return new CouchbaseDatabaseWriter(datasourceConfig);
    }

    public static class CouchbaseDatabaseWriter implements VectorDatabaseWriter, AutoCloseable {

        private final Cluster cluster;
        public final Collection collection;
        private JstlEvaluator idFunction;
        private JstlEvaluator vectorFunction;

        public CouchbaseDatabaseWriter(Map<String, Object> datasourceConfig) {
            String username = (String) datasourceConfig.get("username");
            String password = (String) datasourceConfig.get("password");
            String bucketName = (String) datasourceConfig.get("bucket-name");
            String scopeName = (String) datasourceConfig.getOrDefault("scopeName", "_default");
            String connectionString = (String) datasourceConfig.get("connection-string");
            String collectionName =
                    (String) datasourceConfig.getOrDefault("collectionName", "_default");

            // Create a cluster with the WAN profile
            ClusterOptions clusterOptions =
                    ClusterOptions.clusterOptions(username, password)
                            .environment(
                                    env -> {
                                        env.applyProfile("wan-development");
                                    });

            cluster = Cluster.connect(connectionString, clusterOptions);

            // Get the bucket, scope, and collection
            Bucket bucket = cluster.bucket(bucketName);
            bucket.waitUntilReady(Duration.ofSeconds(10));

            Scope scope = bucket.scope(scopeName);
            collection = scope.collection(collectionName);
        }

        @Override
        public void close() throws Exception {
            cluster.disconnect();
        }

        @Override
        public void initialise(Map<String, Object> agentConfiguration) throws Exception {
            // log.info(
            //         "Initializing CouchbaseDatabaseWriter with configuration: {}",
            //         agentConfiguration);
            this.idFunction = buildEvaluator(agentConfiguration, "vector.id", String.class);
            this.vectorFunction = buildEvaluator(agentConfiguration, "vector.vector", List.class);
        }

        @Override
        public CompletableFuture<Void> upsert(Record record, Map<String, Object> context) {
            CompletableFuture<Void> handle = new CompletableFuture<>();
            return CompletableFuture.runAsync(
                            () -> {
                                try {

                                    MutableRecord mutableRecord =
                                            recordToMutableRecord(record, true);

                                    // Evaluate the ID using the idFunction
                                    String docId =
                                            idFunction != null
                                                    ? (String) idFunction.evaluate(mutableRecord)
                                                    : null;

                                    if (docId == null) {
                                        throw new IllegalArgumentException(
                                                "docId is null, cannot upsert document");
                                    }

                                    Object value = record.value();
                                    Map<String, Object> content;

                                    // Check if the record's value is a Map, otherwise try parsing
                                    // it as JSON
                                    if (value instanceof Map) {
                                        content = (Map<String, Object>) value;
                                    } else if (value instanceof String) {
                                        // Assuming the string is in JSON format
                                        content =
                                                new ObjectMapper()
                                                        .readValue(
                                                                (String) value,
                                                                new TypeReference<
                                                                        Map<String, Object>>() {});
                                    } else {
                                        throw new IllegalArgumentException(
                                                "Record value must be either a Map<String, Object> or a JSON string");
                                    }

                                    // Remove docId from content if present
                                    if (content.containsKey("id")) {
                                        content.remove("id");
                                    }

                                    // Perform the upsert
                                    MutationResult result =
                                            collection.upsert(
                                                    docId, content, UpsertOptions.upsertOptions());

                                    // Logging the result of the upsert operation
                                    log.info("Upsert successful for document ID '{}'", docId);

                                    handle.complete(null); // Completing the future successfully
                                } catch (Exception e) {
                                    log.error(
                                            "Failed to upsert document with ID '{}'",
                                            record.key(),
                                            e);
                                    handle.completeExceptionally(
                                            e); // Completing the future exceptionally
                                }
                            })
                    .exceptionally(
                            e -> {
                                log.error("Exception in upsert operation: ", e);
                                return null;
                            });
        }
    }

    private static JstlEvaluator buildEvaluator(
            Map<String, Object> agentConfiguration, String param, Class type) {
        String expression = agentConfiguration.getOrDefault(param, "").toString();
        if (expression == null || expression.isEmpty()) {
            return null;
        }
        return new JstlEvaluator("${" + expression + "}", type);
    }
}
