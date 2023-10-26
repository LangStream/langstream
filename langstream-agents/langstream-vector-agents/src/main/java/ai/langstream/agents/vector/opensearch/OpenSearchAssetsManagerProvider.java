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
package ai.langstream.agents.vector.opensearch;

import ai.langstream.api.model.AssetDefinition;
import ai.langstream.api.runner.assets.AssetManager;
import ai.langstream.api.runner.assets.AssetManagerProvider;
import ai.langstream.api.util.ConfigurationUtils;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch._types.mapping.TypeMapping;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.CreateIndexResponse;
import org.opensearch.client.opensearch.indices.DeleteIndexRequest;
import org.opensearch.client.opensearch.indices.DeleteIndexResponse;
import org.opensearch.client.opensearch.indices.ExistsRequest;
import org.opensearch.client.opensearch.indices.IndexSettings;
import org.opensearch.client.util.MissingRequiredPropertyException;

@Slf4j
public class OpenSearchAssetsManagerProvider implements AssetManagerProvider {

    @Override
    public boolean supports(String assetType) {
        return "opensearch-index".equals(assetType);
    }

    @Override
    public AssetManager createInstance(String assetType) {

        switch (assetType) {
            case "opensearch-index":
                return new OpenSearchIndexAssetManager();
            default:
                throw new IllegalArgumentException();
        }
    }

    private static class OpenSearchIndexAssetManager implements AssetManager {

        OpenSearchDataSource.OpenSearchQueryStepDataSource datasource;

        AssetDefinition assetDefinition;

        @Override
        public void initialize(AssetDefinition assetDefinition) throws Exception {
            this.datasource = buildDataSource(assetDefinition);
            this.assetDefinition = assetDefinition;
        }

        @Override
        public boolean assetExists() throws Exception {
            String indexName = getIndexName();
            return datasource
                    .getClient()
                    .indices()
                    .exists(new ExistsRequest.Builder().index(indexName).build())
                    .value();
        }

        private String getIndexName() {
            return datasource.getClientConfig().getIndexName();
        }

        @Override
        public void deployAsset() throws Exception {
            final String index = getIndexName();

            final CreateIndexRequest.Builder builder =
                    new CreateIndexRequest.Builder().index(index);
            String settingsJson =
                    ConfigurationUtils.getString("settings", null, assetDefinition.getConfig());
            String mappingsJson =
                    ConfigurationUtils.getString("mappings", null, assetDefinition.getConfig());
            log.info(
                    "Creating index {} with settings {} and mappings {}",
                    index,
                    settingsJson,
                    mappingsJson);
            if (settingsJson != null && !settingsJson.isBlank()) {
                try {
                    final IndexSettings indexSettings =
                            OpenSearchDataSource.parseOpenSearchRequestBodyJson(
                                    settingsJson, IndexSettings._DESERIALIZER);
                    builder.settings(indexSettings);
                } catch (MissingRequiredPropertyException exception) {
                    throw new IllegalArgumentException(
                            "Invalid settings json value " + settingsJson, exception);
                }
            }

            if (mappingsJson != null && !mappingsJson.isBlank()) {
                try {
                    final TypeMapping typeMapping =
                            OpenSearchDataSource.parseOpenSearchRequestBodyJson(
                                    mappingsJson, TypeMapping._DESERIALIZER);
                    builder.mappings(typeMapping);
                } catch (MissingRequiredPropertyException exception) {
                    throw new IllegalArgumentException(
                            "Invalid mappings json value :" + mappingsJson, exception);
                }
            }
            final CreateIndexRequest request = builder.build();
            log.info("Creating index {} ", index);

            final CreateIndexResponse response = datasource.getClient().indices().create(request);
            if (response.acknowledged() != null && response.acknowledged()) {
                log.info("Created index {}", index);
            } else {
                log.error("Failed to create index {}", index);
                throw new RuntimeException(
                        "Failed to create index " + index + " ack=" + response.acknowledged());
            }
        }

        @Override
        public boolean deleteAssetIfExists() throws Exception {
            try {
                final DeleteIndexResponse delete =
                        datasource
                                .getClient()
                                .indices()
                                .delete(
                                        new DeleteIndexRequest.Builder()
                                                .index(getIndexName())
                                                .build());
                if (delete.acknowledged()) {
                    log.info("Deleted index {}", getIndexName());
                    return true;
                } else {
                    log.error("Failed to delete index {}", getIndexName());
                    return false;
                }
            } catch (OpenSearchException exception) {
                if ("index_not_found_exception".equals(exception.error().type())) {
                    return false;
                }
                throw new RuntimeException(exception);
            }
        }

        @Override
        public void close() throws Exception {
            if (datasource != null) {
                datasource.close();
            }
        }
    }

    private static OpenSearchDataSource.OpenSearchQueryStepDataSource buildDataSource(
            AssetDefinition assetDefinition) {
        OpenSearchDataSource dataSource = new OpenSearchDataSource();
        Map<String, Object> datasourceDefinition =
                ConfigurationUtils.getMap("datasource", Map.of(), assetDefinition.getConfig());
        Map<String, Object> configuration =
                ConfigurationUtils.getMap("configuration", Map.of(), datasourceDefinition);
        OpenSearchDataSource.OpenSearchQueryStepDataSource result =
                dataSource.createDataSourceImplementation(configuration);
        result.initialize(null);
        return result;
    }
}
