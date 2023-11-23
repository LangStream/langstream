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
package ai.langstream.agents.vector.astra;

import ai.langstream.api.model.AssetDefinition;
import ai.langstream.api.runner.assets.AssetManager;
import ai.langstream.api.runner.assets.AssetManagerProvider;
import ai.langstream.api.util.ConfigurationUtils;
import io.stargate.sdk.doc.exception.CollectionNotFoundException;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AstraVectorDBAssetsManagerProvider implements AssetManagerProvider {

    @Override
    public boolean supports(String assetType) {
        return "astra-collection".equals(assetType);
    }

    @Override
    public AssetManager createInstance(String assetType) {

        switch (assetType) {
            case "astra-collection":
                return new AstraDBCollectionAssetManager();
            default:
                throw new IllegalArgumentException();
        }
    }

    private abstract static class BaseAstraAssetManager implements AssetManager {

        AstraVectorDBDataSource datasource;
        AssetDefinition assetDefinition;

        @Override
        public void initialize(AssetDefinition assetDefinition) {
            this.datasource = buildDataSource(assetDefinition);
            this.assetDefinition = assetDefinition;
        }

        @Override
        public void close() throws Exception {
            if (datasource != null) {
                datasource.close();
            }
        }
    }

    private static class AstraDBCollectionAssetManager extends BaseAstraAssetManager {

        @Override
        public boolean assetExists() throws Exception {
            String collection = getCollection();
            log.info("Checking if collection {} exists", collection);
            return datasource.getAstraDB().isCollectionExists(collection);
        }

        @Override
        public void deployAsset() throws Exception {
            int vectorDimension = getVectorDimension();

            String collection = getCollection();
            log.info("Create collection {} with vector dimension {}", collection, vectorDimension);
            datasource.getAstraDB().createCollection(collection, vectorDimension);
        }

        private String getCollection() {
            return ConfigurationUtils.getString(
                    "collection-name", null, assetDefinition.getConfig());
        }

        private int getVectorDimension() {
            return ConfigurationUtils.getInt("vector-dimension", 1536, assetDefinition.getConfig());
        }

        @Override
        public boolean deleteAssetIfExists() throws Exception {
            String collection = getCollection();

            log.info("Deleting collection {}", collection);

            try {
                datasource.getAstraDB().deleteCollection(collection);
                return true;
            } catch (CollectionNotFoundException e) {
                log.info(
                        "collection does not exist, maybe it was deleted by another agent ({})",
                        e.toString());
                return false;
            }
        }
    }

    private static AstraVectorDBDataSource buildDataSource(AssetDefinition assetDefinition) {
        AstraVectorDBDataSource dataSource = new AstraVectorDBDataSource();
        Map<String, Object> datasourceDefinition =
                ConfigurationUtils.getMap("datasource", Map.of(), assetDefinition.getConfig());
        Map<String, Object> configuration =
                ConfigurationUtils.getMap("configuration", Map.of(), datasourceDefinition);
        dataSource.initialize(configuration);
        return dataSource;
    }
}
