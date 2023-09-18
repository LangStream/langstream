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
package ai.langstream.impl.common;

import static ai.langstream.api.util.ConfigurationUtils.requiredNonEmptyField;

import ai.langstream.api.model.Application;
import ai.langstream.api.model.AssetDefinition;
import ai.langstream.api.model.Module;
import ai.langstream.api.model.Resource;
import ai.langstream.api.runtime.AssetNode;
import ai.langstream.api.runtime.AssetNodeProvider;
import ai.langstream.api.runtime.ComputeClusterRuntime;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.PluginsRegistry;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/** Utility method to implement an AssetNodeProvider. */
public abstract class AbstractAssetProvider implements AssetNodeProvider {

    private final Set<String> supportedType;

    public AbstractAssetProvider(Set<String> supportedType) {
        this.supportedType = supportedType;
    }

    @Override
    public final AssetNode createImplementation(
            AssetDefinition assetDefinition,
            Module module,
            ExecutionPlan executionPlan,
            ComputeClusterRuntime clusterRuntime,
            PluginsRegistry pluginsRegistry) {
        Map<String, Object> asset =
                planAsset(
                        assetDefinition,
                        executionPlan.getApplication(),
                        clusterRuntime,
                        pluginsRegistry);
        validateAsset(assetDefinition, asset);
        return new AssetNode(asset);
    }

    protected abstract void validateAsset(
            AssetDefinition assetDefinition, Map<String, Object> asset);

    private Map<String, Object> planAsset(
            AssetDefinition assetDefinition,
            Application application,
            ComputeClusterRuntime computeClusterRuntime,
            PluginsRegistry pluginsRegistry) {

        if (!supportedType.contains(assetDefinition.getAssetType())) {
            throw new IllegalStateException();
        }
        Map<String, Resource> resources = application.getResources();
        Map<String, Object> asset = new HashMap<>();
        asset.put("id", assetDefinition.getId());
        asset.put("name", assetDefinition.getName());
        asset.put("asset-type", assetDefinition.getAssetType());
        asset.put("creation-mode", assetDefinition.getCreationMode());
        asset.put("deletion-mode", assetDefinition.getDeletionMode());
        Map<String, Object> configuration = new HashMap<>();
        if (assetDefinition.getConfig() != null) {
            assetDefinition
                    .getConfig()
                    .forEach(
                            (key, value) -> {
                                // automatically resolve resource references
                                // should we do it depending on the asset type ?
                                if (lookupResource(key)) {
                                    String resourceId =
                                            requiredNonEmptyField(
                                                    assetDefinition.getConfig(),
                                                    key,
                                                    describe(assetDefinition));
                                    Resource resource = resources.get(resourceId);
                                    if (resource != null) {
                                        Map<String, Object> resourceImplementation =
                                                computeClusterRuntime.getResourceImplementation(
                                                        resource, pluginsRegistry);
                                        value = Map.of("configuration", resourceImplementation);
                                    } else {
                                        throw new IllegalArgumentException(
                                                "Resource with name="
                                                        + resourceId
                                                        + " not found, declared as "
                                                        + key
                                                        + " in asset "
                                                        + assetDefinition.getId());
                                    }
                                }
                                configuration.put(key, value);
                            });
        }
        asset.put("config", configuration);
        return asset;
    }

    protected abstract boolean lookupResource(String fieldName);

    @Override
    public boolean supports(String type, ComputeClusterRuntime clusterRuntime) {
        return supportedType.contains(type);
    }

    protected static Supplier<String> describe(AssetDefinition assetDefinition) {
        return () ->
                "asset definition, type="
                        + assetDefinition.getAssetType()
                        + ", name="
                        + assetDefinition.getName()
                        + ", id="
                        + assetDefinition.getId();
    }
}
