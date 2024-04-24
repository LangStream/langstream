package ai.langstream.impl.assets;

import ai.langstream.api.doc.AssetConfig;
import ai.langstream.api.doc.ConfigProperty;
import ai.langstream.impl.common.AbstractAssetProvider;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Set;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CouchbaseAssetsProvider extends AbstractAssetProvider {

    public CouchbaseAssetsProvider() {
        super(Set.of("couchbase-bucket"));
    }

    @Override
    protected Class getAssetConfigModelClass(String type) {
        return BucketConfig.class;
    }

    @Override
    protected boolean lookupResource(String fieldName) {
        return "datasource".equals(fieldName);
    }

    @AssetConfig(
            name = "Couchbase bucket",
            description =
                    """
                    Manage a Couchbase bucket.
                    """)
    @Data
    public static class BucketConfig {

        @ConfigProperty(
                description =
                        """
                       Reference to a datasource id configured in the application.
                       """,
                required = true)
        private String datasource;

        @ConfigProperty(
                description =
                        """
                       List of the statement to execute to create the bucket. They will be executed every time the application is deployed or upgraded.
                       """,
                required = true)
        @JsonProperty("create-statements")
        private List<Statement> createStatements;
    }

    @Data
    public static class Statement {
        String api;
        String method;
        String body;
    }
}
