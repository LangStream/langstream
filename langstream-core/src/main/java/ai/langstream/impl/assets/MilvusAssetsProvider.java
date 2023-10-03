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
import ai.langstream.impl.common.AbstractAssetProvider;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Set;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MilvusAssetsProvider extends AbstractAssetProvider {

    public MilvusAssetsProvider() {
        super(Set.of("milvus-collection"));
    }

    @Override
    protected Class getAssetConfigModelClass(String type) {
        return TableConfig.class;
    }

    @Override
    protected boolean lookupResource(String fieldName) {
        return "datasource".equals(fieldName);
    }

    @AssetConfig(
            name = "Milvus collection",
            description =
                    """
                    Manage a Milvus collection.
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
                       Name of the collection.
                       """,
                required = true)
        @JsonProperty("collection-name")
        private String collectionName;

        @ConfigProperty(
                description =
                        """
                       Name of the database where to create the collection.
                       """)
        @JsonProperty("database-name")
        private String databaseName;

        @ConfigProperty(
                description =
                        """
                       List of the statement to execute to create the collection. They will be executed every time the application is deployed or upgraded.
                       """,
                required = true)
        @JsonProperty("create-statements")
        private List<String> createStatements;
    }
}
