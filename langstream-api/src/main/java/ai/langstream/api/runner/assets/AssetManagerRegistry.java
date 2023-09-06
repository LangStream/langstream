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
package ai.langstream.api.runner.assets;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * The runtime registry is a singleton that holds all the runtime information about the possible
 * implementations of the LangStream API.
 */
@Slf4j
public class AssetManagerRegistry {

    private AgentPackageLoader assetManagerPackageLoader;

    public AssetManagerRegistry() {}

    public interface AssetPackage {
        String getName();

        ClassLoader getClassloader();
    }

    public interface AgentPackageLoader {
        AssetPackage loadPackageForAsset(String assetType) throws Exception;

        List<? extends ClassLoader> getAllClassloaders() throws Exception;

        ClassLoader getCustomCodeClassloader();
    }

    @SneakyThrows
    public AssetManagerAndLoader getAssetManager(String assetType) {
        log.info("Loading AssetManager code for type {}", assetType);
        Objects.requireNonNull(assetType, "assetType cannot be null");

        ClassLoader customCodeClassloader =
                assetManagerPackageLoader != null
                        ? assetManagerPackageLoader.getCustomCodeClassloader()
                        : AssetManagerRegistry.class.getClassLoader();
        // always allow to find the system agents
        AssetManagerAndLoader agentCodeProviderProviderFromSystem =
                loadFromClassloader(assetType, customCodeClassloader);
        if (agentCodeProviderProviderFromSystem != null) {
            log.info("The agent {} is loaded from the system classpath", assetType);
            return agentCodeProviderProviderFromSystem;
        }

        if (assetManagerPackageLoader != null) {
            AssetPackage assetPackage = assetManagerPackageLoader.loadPackageForAsset(assetType);

            if (assetPackage != null) {
                log.info("Found the package the agent belongs to: {}", assetPackage.getName());
                AssetManagerAndLoader agentCodeProviderProvider =
                        loadFromClassloader(assetType, assetPackage.getClassloader());
                if (agentCodeProviderProvider == null) {
                    throw new RuntimeException(
                            "Package "
                                    + assetPackage.getName()
                                    + " declared to support agent type "
                                    + assetType
                                    + " but no agent found");
                }
                return agentCodeProviderProvider;
            }

            log.info("No agent found in the package, let's try to find it among all the packages");

            // we are not lucky, let's try to find the agent in all the packages
            List<ClassLoader> candidateClassloaders =
                    new ArrayList<>(assetManagerPackageLoader.getAllClassloaders());

            for (ClassLoader classLoader : candidateClassloaders) {
                AssetManagerAndLoader agentCodeProviderProvider =
                        loadFromClassloader(assetType, classLoader);
                if (agentCodeProviderProvider != null) {
                    return agentCodeProviderProvider;
                }
            }
        }

        throw new RuntimeException("No AgentCodeProvider found for type " + assetType);
    }

    private static AssetManagerAndLoader loadFromClassloader(
            String assetType, ClassLoader classLoader) {
        ServiceLoader<AssetManagerProvider> loader =
                ServiceLoader.load(AssetManagerProvider.class, classLoader);
        Optional<ServiceLoader.Provider<AssetManagerProvider>> agentCodeProviderProvider =
                loader.stream().filter(p -> p.get().supports(assetType)).findFirst();

        return agentCodeProviderProvider
                .map(
                        provider ->
                                new AssetManagerAndLoader(
                                        provider.get().createInstance(assetType), classLoader))
                .orElse(null);
    }

    public void setAssetManagerPackageLoader(AgentPackageLoader loader) {
        this.assetManagerPackageLoader = loader;
    }
}
