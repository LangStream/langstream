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

import ai.langstream.api.model.AssetDefinition;
import ai.langstream.api.runner.assets.AssetManager;
import ai.langstream.api.runner.assets.AssetManagerProvider;
import ai.langstream.api.util.ConfigurationUtils;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.query.QueryResult;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CouchbaseAssetsManagerProvider implements AssetManagerProvider {

    @Override
    public boolean supports(String assetType) {
        return "couchbase-bucket".equals(assetType);
    }

    @Override
    public AssetManager createInstance(String assetType) {
        switch (assetType) {
            case "couchbase-bucket":
                return new CouchbaseBucketAssetManager();
            default:
                throw new IllegalArgumentException("Unsupported asset type: " + assetType);
        }
    }

    private static class CouchbaseBucketAssetManager implements AssetManager {

        private Cluster cluster;
        private Bucket bucket;
        private AssetDefinition assetDefinition;

        @Override
        public void initialize(AssetDefinition assetDefinition) throws Exception {
            this.assetDefinition = assetDefinition;
            String connectionString =
                    ConfigurationUtils.getString(
                            "connectionString", "", assetDefinition.getConfig());
            String username =
                    ConfigurationUtils.getString("username", "", assetDefinition.getConfig());
            String password =
                    ConfigurationUtils.getString("password", "", assetDefinition.getConfig());
            this.cluster = Cluster.connect(connectionString, username, password);
            this.bucket =
                    cluster.bucket(
                            ConfigurationUtils.getString(
                                    "bucketName", "", assetDefinition.getConfig()));
        }

        @Override
        public boolean assetExists() throws Exception {
            try {
                // Assuming asset refers to a bucket's existence
                bucket.waitUntilReady(java.time.Duration.ofSeconds(30));
                return true;
            } catch (Exception e) {
                log.info("Bucket does not exist or is not ready: {}", e.getMessage());
                return false;
            }
        }

        @Override
        public void deployAsset() throws Exception {
            Object rawStatements = assetDefinition.getConfig().get("create-statements");
            if (rawStatements instanceof List) {
                List<?> listStatements = (List<?>) rawStatements;
                List<Map<String, Object>> statements =
                        listStatements.stream()
                                .filter(item -> item instanceof Map)
                                .map(item -> (Map<String, Object>) item)
                                .collect(Collectors.toList());

                if (statements.isEmpty()) {
                    log.info("No create-statements found in configuration.");
                    return;
                }

                for (Map<String, Object> statement : statements) {
                    String n1ql = (String) statement.get("n1ql");
                    if (n1ql == null || n1ql.trim().isEmpty()) {
                        log.warn("Skipping a statement due to missing 'n1ql' key or empty value.");
                        continue;
                    }

                    try {
                        QueryResult result = cluster.query(n1ql);
                        result.rowsAsObject()
                                .forEach(row -> log.info("Query executed with row: {}", row));
                    } catch (Exception e) {
                        log.error("Failed to execute N1QL statement: " + n1ql, e);
                    }
                }
            } else {
                log.error(
                        "Expected 'create-statements' to be a list of maps, but found: {}",
                        rawStatements != null ? rawStatements.getClass().getSimpleName() : "null");
            }
        }

        @Override
        public boolean deleteAssetIfExists() throws Exception {
            if (!assetExists()) {
                return false;
            }
            // Example: drop a bucket (not typically done programmatically, careful with
            // permissions)
            // Normally you would delete specific documents or secondary indexes
            String dropStatement = "DROP BUCKET `" + bucket.name() + "`;";
            cluster.query(dropStatement);
            log.info("Bucket {} dropped", bucket.name());
            return true;
        }

        @Override
        public void close() throws Exception {
            if (cluster != null) {
                cluster.disconnect();
            }
        }
    }
}
