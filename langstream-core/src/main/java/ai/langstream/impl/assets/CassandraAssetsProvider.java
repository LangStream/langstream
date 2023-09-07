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

import ai.langstream.api.model.AssetDefinition;
import ai.langstream.api.util.ConfigurationUtils;
import ai.langstream.impl.common.AbstractAssetProvider;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CassandraAssetsProvider extends AbstractAssetProvider {

    public CassandraAssetsProvider() {
        super(Set.of("cassandra-table", "cassandra-keyspace"));
    }

    @Override
    protected void validateAsset(AssetDefinition assetDefinition, Map<String, Object> asset) {
        Map<String, Object> configuration = ConfigurationUtils.getMap("config", null, asset);
        switch (assetDefinition.getAssetType()) {
            case "cassandra-table" -> {
                requiredNonEmptyField(assetDefinition, configuration, "table-name");
                requiredNonEmptyField(assetDefinition, configuration, "keyspace");
                requiredListField(assetDefinition, configuration, "create-statements");
                requiredField(assetDefinition, configuration, "datasource");
            }
            case "cassandra-keyspace" -> {
                requiredNonEmptyField(assetDefinition, configuration, "keyspace");
                requiredListField(assetDefinition, configuration, "create-statements");
                requiredField(assetDefinition, configuration, "datasource");
            }
            default -> throw new IllegalStateException();
        }
    }

    @Override
    protected boolean lookupResource(String fieldName) {
        return "datasource".contains(fieldName);
    }
}
