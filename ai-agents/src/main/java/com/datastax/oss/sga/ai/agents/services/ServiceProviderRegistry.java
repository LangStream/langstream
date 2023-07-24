package com.datastax.oss.sga.ai.agents.services;

import com.datastax.oss.streaming.ai.services.ServiceProvider;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.ServiceLoader;

/**
 * This is the API to load a CodeStorage implementation.
 */
@Slf4j
public class ServiceProviderRegistry {

    public static ServiceProvider getServiceProvider(Map<String, Object> agentConfiguration) {
        if (agentConfiguration == null || agentConfiguration.isEmpty()) {
            return null;
        }
        log.info("Loading AI ServiceProvider implementation for {}", agentConfiguration);

        ServiceLoader<ServiceProviderProvider> loader = ServiceLoader.load(ServiceProviderProvider.class);
        ServiceLoader.Provider<ServiceProviderProvider> provider = loader
                .stream()
                .filter(p -> {
                    return p.get().supports(agentConfiguration);
                })
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No DataSource found for resource " + agentConfiguration));

        return provider.get().createImplementation(agentConfiguration);
    }


}
