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
public class JdbcAssetsProvider extends AbstractAssetProvider {

    public JdbcAssetsProvider() {
        super(Set.of("jdbc-table"));
    }

    @Override
    protected Class getAssetConfigModelClass(String type) {
        return TableConfig.class;
    }

    @Override
    protected void validateAsset(AssetDefinition assetDefinition, Map<String, Object> asset) {
        Map<String, Object> configuration = ConfigurationUtils.getMap("config", null, asset);
        switch (assetDefinition.getAssetType()) {
            case "jdbc-table" -> {
                checkDeleteStatements(assetDefinition, configuration);
            }
            default -> throw new IllegalStateException(
                    "Unexpected value: " + assetDefinition.getAssetType());
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
            name = "JDBC table",
            description = """
                    Manage a JDBC table.
                    """)
    @Data
    public static class TableConfig {

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
}
