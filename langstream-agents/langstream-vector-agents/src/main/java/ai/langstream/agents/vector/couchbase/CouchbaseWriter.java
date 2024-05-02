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

import ai.langstream.api.database.VectorDatabaseWriter;
import ai.langstream.api.database.VectorDatabaseWriterProvider;
import ai.langstream.api.runner.code.Record;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.kv.UpsertOptions;
import java.time.Duration;
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

        public CouchbaseDatabaseWriter(Map<String, Object> datasourceConfig) {
            String connectionString = (String) datasourceConfig.get("connectionString");
            String username = (String) datasourceConfig.get("username");
            String password = (String) datasourceConfig.get("password");
            String bucketName = (String) datasourceConfig.get("bucketName");
            String scopeName = (String) datasourceConfig.getOrDefault("scopeName", "_default");
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
            // Additional initializations can be implemented here
        }

        @Override
        public CompletableFuture<Void> upsert(Record record, Map<String, Object> context) {
            return CompletableFuture.runAsync(
                    () -> {
                        try {
                            String docId = record.key().toString();
                            Map<String, Object> content;
                            if (record.value() instanceof Map) {
                                content = (Map<String, Object>) record.value();
                            } else {
                                throw new IllegalArgumentException(
                                        "Record value must be a Map<String, Object>");
                            }

                            collection.upsert(docId, content, UpsertOptions.upsertOptions());
                        } catch (Exception e) {
                            log.error("Failed to upsert document", e);
                            throw new RuntimeException("Failed to upsert document", e);
                        }
                    });
        }
    }
}
