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

import ai.langstream.api.util.ClassloaderUtils;
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

    private AssetManagerPackageLoader assetManagerPackageLoader;

    public AssetManagerRegistry() {}

    public interface AssetPackage {
        String getName();

        ClassLoader getClassloader();
    }

    public interface AssetManagerPackageLoader {
        AssetPackage loadPackageForAsset(String assetType) throws Exception;

        List<? extends ClassLoader> getAllClassloaders() throws Exception;

        ClassLoader getSystemClassloader();
    }

    @SneakyThrows
    public AssetManagerAndLoader getAssetManager(String assetType) {
        log.info("Loading AssetManager code for type {}", assetType);
        Objects.requireNonNull(assetType, "assetType cannot be null");

        ClassLoader customCodeClassloader =
                assetManagerPackageLoader != null
                        ? assetManagerPackageLoader.getSystemClassloader()
                        : AssetManagerRegistry.class.getClassLoader();
        // always allow to find the system assets
        AssetManagerAndLoader assetCodeProviderProviderFromSystem =
                loadFromClassloader(assetType, customCodeClassloader);
        if (assetCodeProviderProviderFromSystem != null) {
            log.info("The asset manager {} is loaded from the system classpath", assetType);
            return assetCodeProviderProviderFromSystem;
        }

        if (assetManagerPackageLoader != null) {
            AssetPackage assetPackage = assetManagerPackageLoader.loadPackageForAsset(assetType);

            if (assetPackage != null) {
                log.info("Found the package the asset belongs to: {}", assetPackage.getName());
                AssetManagerAndLoader assetCodeProviderProvider =
                        loadFromClassloader(assetType, assetPackage.getClassloader());
                if (assetCodeProviderProvider == null) {
                    throw new RuntimeException(
                            "Package "
                                    + assetPackage.getName()
                                    + " declared to support asset type "
                                    + assetType
                                    + " but no asset manager found");
                }
                return assetCodeProviderProvider;
            }

            log.info(
                    "No asset manager found in the package, let's try to find it among all the packages");

            // we are not lucky, let's try to find the asset in all the packages
            List<ClassLoader> candidateClassloaders =
                    new ArrayList<>(assetManagerPackageLoader.getAllClassloaders());

            for (ClassLoader classLoader : candidateClassloaders) {
                AssetManagerAndLoader assetManagerCodeProviderProvider =
                        loadFromClassloader(assetType, classLoader);
                if (assetManagerCodeProviderProvider != null) {
                    return assetManagerCodeProviderProvider;
                }
            }
        }

        throw new RuntimeException("No AssetManagerProvider found for type " + assetType);
    }

    private static AssetManagerAndLoader loadFromClassloader(
            String assetType, ClassLoader classLoader) {
        ServiceLoader<AssetManagerProvider> loader =
                ServiceLoader.load(AssetManagerProvider.class, classLoader);
        Optional<ServiceLoader.Provider<AssetManagerProvider>> assetManagerCodeProviderProvider =
                loader.stream().filter(p -> p.get().supports(assetType)).findFirst();

        return assetManagerCodeProviderProvider
                .map(
                        provider ->
                                new AssetManagerAndLoader(
                                        ClassloaderUtils.executeWithClassloader(
                                                classLoader,
                                                () -> provider.get().createInstance(assetType)),
                                        classLoader))
                .orElse(null);
    }

    public void setAssetManagerPackageLoader(AssetManagerPackageLoader loader) {
        this.assetManagerPackageLoader = loader;
    }
}
