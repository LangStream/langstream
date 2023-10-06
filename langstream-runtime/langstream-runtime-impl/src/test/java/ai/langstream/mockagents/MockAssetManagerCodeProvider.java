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
package ai.langstream.mockagents;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.langstream.api.model.AssetDefinition;
import ai.langstream.api.runner.assets.AssetManager;
import ai.langstream.api.runner.assets.AssetManagerProvider;
import ai.langstream.api.util.ConfigurationUtils;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MockAssetManagerCodeProvider implements AssetManagerProvider {
    @Override
    public boolean supports(String agentType) {
        return "mock-database-resource".equals(agentType);
    }

    @Override
    public AssetManager createInstance(String agentType) {
        switch (agentType) {
            case "mock-database-resource":
                return new MockDatabaseResourceAssetManager();
            default:
                throw new IllegalStateException();
        }
    }

    public static class MockDatabaseResourceAssetManager implements AssetManager {

        public static CopyOnWriteArrayList<AssetDefinition> DEPLOYED_ASSETS =
                new CopyOnWriteArrayList<>();

        private AssetDefinition assetDefinition;

        @Override
        public void initialize(AssetDefinition assetDefinition) throws Exception {
            this.assetDefinition = assetDefinition;
        }

        @Override
        public synchronized boolean assetExists() throws Exception {
            return DEPLOYED_ASSETS.stream()
                    .anyMatch(a -> a.getId().equals(assetDefinition.getId()));
        }

        @Override
        public synchronized void deployAsset() throws Exception {
            log.info("Deploying asset {}", assetDefinition);
            Map<String, Object> datasource =
                    ConfigurationUtils.getMap("datasource", null, assetDefinition.getConfig());
            if (datasource == null) {
                throw new IllegalStateException("Datasource is required");
            }
            Map<String, Object> configuration =
                    (Map<String, Object>) datasource.get("configuration");
            if (configuration == null) {
                throw new IllegalStateException("Datasource configuration is required");
            }
            assertEquals("bar", configuration.get("url"));
            DEPLOYED_ASSETS.add(assetDefinition);
        }

        @Override
        public synchronized boolean deleteAssetIfExists() throws Exception {
            log.info("Deleting asset {}", assetDefinition);
            return DEPLOYED_ASSETS.remove(assetDefinition);
        }
    }
}
