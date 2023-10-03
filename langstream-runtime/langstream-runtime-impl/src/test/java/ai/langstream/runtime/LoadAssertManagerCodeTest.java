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
package ai.langstream.runtime;

import static org.junit.jupiter.api.Assertions.assertFalse;

import ai.langstream.api.model.AssetDefinition;
import ai.langstream.api.runner.assets.AssetManager;
import ai.langstream.api.runner.assets.AssetManagerRegistry;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
class LoadAssertManagerCodeTest {

    @Test
    public void testLoadMockAsset() throws Exception {
        AssetManagerRegistry registry = new AssetManagerRegistry();
        AssetDefinition assetDefinition = new AssetDefinition();
        assetDefinition.setConfig(
                Map.of("datasource", Map.of("configuration", Map.of("url", "bar"))));
        assetDefinition.setAssetType("mock-database-resource");
        AssetManager assetManager =
                registry.getAssetManager(assetDefinition.getAssetType()).agentCode();

        assetManager.initialize(assetDefinition);
        assertFalse(assetManager.assetExists());
        assetManager.deployAsset();
        assetManager.close();
    }
}
