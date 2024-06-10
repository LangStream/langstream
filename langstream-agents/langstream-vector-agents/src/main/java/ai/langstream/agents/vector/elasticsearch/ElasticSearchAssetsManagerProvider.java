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
package ai.langstream.agents.vector.elasticsearch;

import ai.langstream.api.model.AssetDefinition;
import ai.langstream.api.runner.assets.AssetManager;
import ai.langstream.api.runner.assets.AssetManagerProvider;
import ai.langstream.api.util.ConfigurationUtils;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch.indices.*;
import co.elastic.clients.util.MissingRequiredPropertyException;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ElasticSearchAssetsManagerProvider implements AssetManagerProvider {

    @Override
    public boolean supports(String assetType) {
        return "elasticsearch-index".equals(assetType);
    }

    @Override
    public AssetManager createInstance(String assetType) {

        switch (assetType) {
            case "elasticsearch-index":
                return new ElasticSearchIndexAssetManager();
            default:
                throw new IllegalArgumentException();
        }
    }

    private static class ElasticSearchIndexAssetManager implements AssetManager {

        ElasticSearchDataSource.ElasticSearchQueryStepDataSource datasource;

        AssetDefinition assetDefinition;

        private String indexName;

        @Override
        public void initialize(AssetDefinition assetDefinition) {
            this.datasource = buildDataSource(assetDefinition);
            this.assetDefinition = assetDefinition;
            this.indexName =
                    ConfigurationUtils.getString("index", null, assetDefinition.getConfig());
            if (indexName == null) {
                throw new IllegalArgumentException("Missing 'index' name");
            }
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
            return indexName;
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
                            ElasticSearchDataSource.parseElasticSearchRequestBodyJson(
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
                            ElasticSearchDataSource.parseElasticSearchRequestBodyJson(
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
            if (response.acknowledged()) {
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
            } catch (ElasticsearchException exception) {
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

    private static ElasticSearchDataSource.ElasticSearchQueryStepDataSource buildDataSource(
            AssetDefinition assetDefinition) {
        ElasticSearchDataSource dataSource = new ElasticSearchDataSource();
        Map<String, Object> datasourceDefinition =
                ConfigurationUtils.getMap("datasource", Map.of(), assetDefinition.getConfig());
        Map<String, Object> configuration =
                ConfigurationUtils.getMap("configuration", Map.of(), datasourceDefinition);
        ElasticSearchDataSource.ElasticSearchQueryStepDataSource result =
                dataSource.createDataSourceImplementation(configuration);
        result.initialize(null);
        return result;
    }
}
