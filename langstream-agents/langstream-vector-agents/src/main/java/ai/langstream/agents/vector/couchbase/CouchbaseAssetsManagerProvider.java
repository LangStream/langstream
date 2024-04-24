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
            List<Map<String, Object>> statements =
                    ConfigurationUtils.getMaps("create-statements", assetDefinition.getConfig());

            if (statements.isEmpty()) {
                log.info("No create-statements found in configuration.");
                return; // Early return if there are no statements to process.
            }

            for (Map<String, Object> statement : statements) {
                String n1ql = (String) statement.get("n1ql");
                if (n1ql == null || n1ql.trim().isEmpty()) {
                    log.warn("Skipping a statement due to missing 'n1ql' key or empty value.");
                    continue;
                }

                try {
                    QueryResult result = cluster.query(n1ql);
                    log.info("Query executed with result: {}", result.allRows());
                } catch (Exception e) {
                    log.error("Failed to execute N1QL statement: " + n1ql, e);
                    // Depending on your error handling policy, you might want to continue or abort
                    // here.
                }
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
