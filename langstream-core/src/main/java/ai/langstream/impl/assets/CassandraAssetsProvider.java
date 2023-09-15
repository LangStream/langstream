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

import static ai.langstream.api.util.ConfigurationUtils.requiredField;
import static ai.langstream.api.util.ConfigurationUtils.requiredListField;
import static ai.langstream.api.util.ConfigurationUtils.requiredNonEmptyField;

import ai.langstream.api.model.AssetDefinition;
import ai.langstream.api.util.ConfigurationUtils;
import ai.langstream.impl.common.AbstractAssetProvider;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CassandraAssetsProvider extends AbstractAssetProvider {

    public CassandraAssetsProvider() {
        super(Set.of("cassandra-table", "cassandra-keyspace", "astra-keyspace"));
    }

    @Override
    protected void validateAsset(AssetDefinition assetDefinition, Map<String, Object> asset) {
        Map<String, Object> configuration = ConfigurationUtils.getMap("config", null, asset);
        requiredField(configuration, "datasource", describe(assetDefinition));
        final Map<String, Object> datasource =
                ConfigurationUtils.getMap("datasource", Map.of(), configuration);
        final Map<String, Object> datasourceConfiguration =
                ConfigurationUtils.getMap("configuration", Map.of(), datasource);
        switch (assetDefinition.getAssetType()) {
            case "cassandra-table" -> {
                requiredNonEmptyField(configuration, "table-name", describe(assetDefinition));
                requiredNonEmptyField(configuration, "keyspace", describe(assetDefinition));
                requiredListField(configuration, "create-statements", describe(assetDefinition));
                checkDeleteStatements(assetDefinition, configuration);
            }
            case "cassandra-keyspace" -> {
                requiredNonEmptyField(configuration, "keyspace", describe(assetDefinition));
                requiredListField(configuration, "create-statements", describe(assetDefinition));
                checkDeleteStatements(assetDefinition, configuration);
                if (datasourceConfiguration.containsKey("secureBundle")
                        || datasourceConfiguration.containsKey("database")) {
                    throw new IllegalArgumentException("Use astra-keyspace for AstraDB services");
                }
            }
            case "astra-keyspace" -> {
                requiredNonEmptyField(configuration, "keyspace", describe(assetDefinition));
                if (!datasourceConfiguration.containsKey("secureBundle")
                        && !datasourceConfiguration.containsKey("database")) {
                    throw new IllegalArgumentException(
                            "Use cassandra-keyspace for a standard Cassandra service (not AstraDB)");
                }
                // are we are using the AstraDB SDK we need also the AstraCS token and
                // the name of the database
                requiredNonEmptyField(datasourceConfiguration, "token", describe(assetDefinition));
                requiredNonEmptyField(
                        datasourceConfiguration, "database", describe(assetDefinition));
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
        return "datasource".contains(fieldName);
    }
}
