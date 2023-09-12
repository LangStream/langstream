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
package ai.langstream.api.runner.topics;

import ai.langstream.api.model.StreamingCluster;
import ai.langstream.api.runner.code.AgentCodeRegistry;
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
public class TopicConnectionsRuntimeRegistry {

    private TopicConnectionsPackageLoader packageLoader;

    public interface TopicConnectionsPackageLoader {
        TopicConnectionsRuntimePackage loadPackageForTopicConnectionRuntime(String type)
                throws Exception;

        List<? extends ClassLoader> getAllClassloaders() throws Exception;

        ClassLoader getSystemClassloader();
    }

    public interface TopicConnectionsRuntimePackage {
        String getName();

        ClassLoader getClassloader();
    }

    public TopicConnectionsRuntimeRegistry setPackageLoader(
            TopicConnectionsPackageLoader packageLoader) {
        this.packageLoader = packageLoader;
        return this;
    }

    @SneakyThrows
    public TopicConnectionsRuntimeAndLoader getTopicConnectionsRuntime(
            StreamingCluster streamingCluster) {
        Objects.requireNonNull(streamingCluster, "streamingCluster cannot be null");
        Objects.requireNonNull(streamingCluster.type(), "streamingCluster type cannot be null");
        log.info("Loading code for type {}", streamingCluster.type());
        String type = streamingCluster.type();

        ClassLoader customCodeClassloader =
                packageLoader != null
                        ? packageLoader.getSystemClassloader()
                        : AgentCodeRegistry.class.getClassLoader();
        // always allow to find the system implementations
        TopicConnectionsRuntimeAndLoader codeProviderProviderFromSystem =
                loadFromClassloader(type, customCodeClassloader);
        if (codeProviderProviderFromSystem != null) {
            log.info("The agent {} is loaded from the system classpath", type);
            return codeProviderProviderFromSystem;
        }

        if (packageLoader != null) {
            TopicConnectionsRuntimePackage agentPackage =
                    packageLoader.loadPackageForTopicConnectionRuntime(type);

            if (agentPackage != null) {
                log.info("Found the package the type belongs to: {}", agentPackage.getName());
                TopicConnectionsRuntimeAndLoader codeProviderProvider =
                        loadFromClassloader(type, agentPackage.getClassloader());
                if (codeProviderProvider == null) {
                    throw new RuntimeException(
                            "Package "
                                    + agentPackage.getName()
                                    + " declared to support type "
                                    + type
                                    + " but no implementation found");
                }
                return codeProviderProvider;
            }

            log.info(
                    "No implementation found in the package, let's try to find it among all the packages");

            // we are not lucky, let's try to find the agent in all the packages
            List<ClassLoader> candidateClassloaders =
                    new ArrayList<>(packageLoader.getAllClassloaders());

            for (ClassLoader classLoader : candidateClassloaders) {
                TopicConnectionsRuntimeAndLoader codeProviderProvider =
                        loadFromClassloader(type, classLoader);
                if (codeProviderProvider != null) {
                    return codeProviderProvider;
                }
            }
        }

        throw new RuntimeException("No TopicConnectionsRuntimeProvider found for type " + type);
    }

    private static TopicConnectionsRuntimeAndLoader loadFromClassloader(
            String type, ClassLoader classLoader) {
        ServiceLoader<TopicConnectionsRuntimeProvider> loader =
                ServiceLoader.load(TopicConnectionsRuntimeProvider.class, classLoader);
        Optional<ServiceLoader.Provider<TopicConnectionsRuntimeProvider>>
                agentCodeProviderProvider =
                        loader.stream().filter(p -> p.get().supports(type)).findFirst();

        return agentCodeProviderProvider
                .map(
                        provider ->
                                new TopicConnectionsRuntimeAndLoader(
                                        ClassloaderUtils.executeWithClassloader(
                                                classLoader,
                                                () -> provider.get().getImplementation()),
                                        classLoader))
                .orElse(null);
    }
}
