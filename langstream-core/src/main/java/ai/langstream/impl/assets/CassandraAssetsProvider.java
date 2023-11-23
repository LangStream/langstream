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
package ai.langstream.impl.assets;

import static ai.langstream.api.util.ConfigurationUtils.requiredListField;
import static ai.langstream.api.util.ConfigurationUtils.requiredNonEmptyField;

import ai.langstream.api.doc.AssetConfig;
import ai.langstream.api.doc.ConfigProperty;
import ai.langstream.api.model.AssetDefinition;
import ai.langstream.api.util.ConfigurationUtils;
import ai.langstream.impl.common.AbstractAssetProvider;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CassandraAssetsProvider extends AbstractAssetProvider {

    protected static final String CASSANDRA_TABLE = "cassandra-table";
    protected static final String CASSANDRA_KEYSPACE = "cassandra-keyspace";
    protected static final String ASTRA_KEYSPACE = "astra-keyspace";

    public CassandraAssetsProvider() {
        super(Set.of(CASSANDRA_TABLE, CASSANDRA_KEYSPACE, ASTRA_KEYSPACE));
    }

    @Override
    protected Class getAssetConfigModelClass(String type) {
        return switch (type) {
            case CASSANDRA_TABLE -> CassandraTableConfig.class;
            case CASSANDRA_KEYSPACE -> CassandraKeyspaceConfig.class;
            case ASTRA_KEYSPACE -> AstraKeyspaceConfig.class;
            default -> throw new IllegalArgumentException("Unknown asset type " + type);
        };
    }

    @Override
    protected void validateAsset(AssetDefinition assetDefinition, Map<String, Object> asset) {
        Map<String, Object> configuration = ConfigurationUtils.getMap("config", null, asset);
        final Map<String, Object> datasource =
                ConfigurationUtils.getMap("datasource", Map.of(), configuration);
        final Map<String, Object> datasourceConfiguration =
                ConfigurationUtils.getMap("configuration", Map.of(), datasource);
        switch (assetDefinition.getAssetType()) {
            case CASSANDRA_TABLE -> {
                checkDeleteStatements(assetDefinition, configuration);
            }
            case CASSANDRA_KEYSPACE -> {
                checkDeleteStatements(assetDefinition, configuration);
                if (datasourceConfiguration.containsKey("secureBundle")
                        || datasourceConfiguration.containsKey("database")) {
                    throw new IllegalArgumentException("Use astra-keyspace for AstraDB services");
                }
            }
            case ASTRA_KEYSPACE -> {
                if (!datasourceConfiguration.containsKey("secureBundle")
                        && !datasourceConfiguration.containsKey("database")
                        && !datasourceConfiguration.containsKey("database-id")) {
                    throw new IllegalArgumentException(
                            "Use cassandra-keyspace for a standard Cassandra service (not AstraDB)");
                }
                // are we are using the AstraDB SDK we need also the AstraCS token and
                // the name of the database
                requiredNonEmptyField(datasourceConfiguration, "token", describe(assetDefinition));
                if (!datasourceConfiguration.containsKey("database")) {
                    requiredNonEmptyField(
                            datasourceConfiguration, "database-id", describe(assetDefinition));
                }
                if (!datasourceConfiguration.containsKey("database-id")) {
                    requiredNonEmptyField(
                            datasourceConfiguration, "database", describe(assetDefinition));
                }
            }
            default -> {}
        }
    }

    private static void checkDeleteStatements(
            AssetDefinition assetDefinition, Map<String, Object> configuration) {
        if (AssetDefinition.DELETE_MODE_DELETE.equals(assetDefinition.getDeletionMode())) {
            requiredListField(configuration, "delete-statements", describe(assetDefinition));
        } else {
            if (configuration.containsKey("delete-statements")) {
                throw new IllegalArgumentException(
                        "delete-statements must be set only if deletion-mode=delete");
            }
        }
    }

    @Override
    protected boolean lookupResource(String fieldName) {
        return "datasource".equals(fieldName);
    }

    @AssetConfig(
            name = "Cassandra table",
            description =
                    """
                    Manage a Cassandra table in existing keyspace.
                    """)
    @Data
    public static class CassandraTableConfig {

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
                       Name of the table.
                       """,
                required = true)
        @JsonProperty("table-name")
        private String table;

        @ConfigProperty(
                description =
                        """
                       Name of the keyspace where the table is located.
                       """,
                required = true)
        private String keyspace;

        @ConfigProperty(
                description =
                        """
                       List of the statement to execute to create the table. They will be executed every time the application is deployed or upgraded.
                       """,
                required = true)
        @JsonProperty("create-statements")
        private List<String> createStatements;

        @ConfigProperty(
                description =
                        """
                       List of the statement to execute to cleanup the table. They will be executed when the application is deleted only if 'deletion-mode' is 'delete'.
                       """)
        @JsonProperty("delete-statements")
        private List<String> deleteStatements;
    }

    @AssetConfig(
            name = "Cassandra keyspace",
            description =
                    """
                    Manage a Cassandra keyspace.
                    """)
    @Data
    public static class CassandraKeyspaceConfig {

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
                       Name of the keyspace to create.
                       """,
                required = true)
        private String keyspace;

        @ConfigProperty(
                description =
                        """
                       List of the statement to execute to create the keyspace. They will be executed every time the application is deployed or upgraded.
                       """,
                required = true)
        @JsonProperty("create-statements")
        private List<String> createStatements;

        @ConfigProperty(
                description =
                        """
                       List of the statement to execute to cleanup the keyspace. They will be executed when the application is deleted only if 'deletion-mode' is 'delete'.
                       """)
        @JsonProperty("delete-statements")
        private List<String> deleteStatements;
    }

    @AssetConfig(
            name = "Astra keyspace",
            description =
                    """
                    Manage a DataStax Astra keyspace.
                    """)
    @Data
    public static class AstraKeyspaceConfig {

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
                       Name of the keyspace to create.
                       """,
                required = true)
        private String keyspace;
    }
}
