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
package ai.langstream.api.runner.code;

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
public class AgentCodeRegistry {

    private AgentPackageLoader agentPackageLoader;

    public AgentCodeRegistry() {}

    public interface AgentPackage {
        String getName();

        ClassLoader getClassloader();
    }

    public interface AgentPackageLoader {
        AgentPackage loadPackageForAgent(String agentType) throws Exception;

        List<? extends ClassLoader> getAllClassloaders() throws Exception;

        ClassLoader getSystemClassloader();
    }

    @SneakyThrows
    public AgentCodeAndLoader getAgentCode(String agentType) {
        log.info("Loading agent code for type {}", agentType);
        Objects.requireNonNull(agentType, "agentType cannot be null");

        ClassLoader customCodeClassloader =
                agentPackageLoader != null
                        ? agentPackageLoader.getSystemClassloader()
                        : AgentCodeRegistry.class.getClassLoader();
        // always allow to find the system agents
        AgentCodeAndLoader agentCodeProviderProviderFromSystem =
                loadFromClassloader(agentType, customCodeClassloader);
        if (agentCodeProviderProviderFromSystem != null) {
            log.info("The agent {} is loaded from the system classpath", agentType);
            return agentCodeProviderProviderFromSystem;
        }

        if (agentPackageLoader != null) {
            AgentPackage agentPackage = agentPackageLoader.loadPackageForAgent(agentType);

            if (agentPackage != null) {
                log.info("Found the package the agent belongs to: {}", agentPackage.getName());
                AgentCodeAndLoader agentCodeProviderProvider =
                        loadFromClassloader(agentType, agentPackage.getClassloader());
                if (agentCodeProviderProvider == null) {
                    throw new RuntimeException(
                            "Package "
                                    + agentPackage.getName()
                                    + " declared to support agent type "
                                    + agentType
                                    + " but no agent found");
                }
                return agentCodeProviderProvider;
            }

            log.info("No agent found in the package, let's try to find it among all the packages");

            // we are not lucky, let's try to find the agent in all the packages
            List<ClassLoader> candidateClassloaders =
                    new ArrayList<>(agentPackageLoader.getAllClassloaders());

            for (ClassLoader classLoader : candidateClassloaders) {
                AgentCodeAndLoader agentCodeProviderProvider =
                        loadFromClassloader(agentType, classLoader);
                if (agentCodeProviderProvider != null) {
                    return agentCodeProviderProvider;
                }
            }
        }

        throw new RuntimeException("No AgentCodeProvider found for type " + agentType);
    }

    private static AgentCodeAndLoader loadFromClassloader(
            String agentType, ClassLoader classLoader) {
        ServiceLoader<AgentCodeProvider> loader =
                ServiceLoader.load(AgentCodeProvider.class, classLoader);
        Optional<ServiceLoader.Provider<AgentCodeProvider>> agentCodeProviderProvider =
                loader.stream().filter(p -> p.get().supports(agentType)).findFirst();

        return agentCodeProviderProvider
                .map(
                        provider -> {
                            AgentCode instance =
                                    ClassloaderUtils.executeWithClassloader(
                                            classLoader,
                                            () -> provider.get().createInstance(agentType));
                            return new AgentCodeAndLoader(instance, classLoader);
                        })
                .orElse(null);
    }

    public void setAgentPackageLoader(AgentPackageLoader loader) {
        this.agentPackageLoader = loader;
    }
}
