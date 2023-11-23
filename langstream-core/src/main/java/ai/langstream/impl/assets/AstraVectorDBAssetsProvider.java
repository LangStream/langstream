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

import ai.langstream.api.doc.AssetConfig;
import ai.langstream.api.doc.ConfigProperty;
import ai.langstream.api.model.AssetDefinition;
import ai.langstream.api.util.ConfigurationUtils;
import ai.langstream.impl.common.AbstractAssetProvider;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import java.util.Set;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AstraVectorDBAssetsProvider extends AbstractAssetProvider {

    protected static final String ASTRA_COLLECTION = "astra-collection";

    public AstraVectorDBAssetsProvider() {
        super(Set.of(ASTRA_COLLECTION));
    }

    @Override
    protected Class getAssetConfigModelClass(String type) {
        return switch (type) {
            case ASTRA_COLLECTION -> AstraCollectionConfig.class;
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
            default -> {}
        }
    }

    @Override
    protected boolean lookupResource(String fieldName) {
        return "datasource".equals(fieldName);
    }

    @AssetConfig(
            name = "Astra Collection",
            description =
                    """
                    Manage a DataStax Astra Collection.
                    """)
    @Data
    public static class AstraCollectionConfig {

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
                       Name of the collection to create.
                       """,
                required = true)
        @JsonProperty("collection-name")
        private String collectionName;

        @ConfigProperty(
                description =
                        """
                       Size of the vector.
                       """,
                required = true)
        @JsonProperty("vector-dimension")
        private int vectorDimension;
    }
}
