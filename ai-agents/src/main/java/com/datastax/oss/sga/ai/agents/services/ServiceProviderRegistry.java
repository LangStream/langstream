package com.datastax.oss.sga.ai.agents.services;

import com.datastax.oss.streaming.ai.completions.CompletionsService;
import com.datastax.oss.streaming.ai.embeddings.EmbeddingsService;
import com.datastax.oss.streaming.ai.services.ServiceProvider;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;

/**
 * This is the API to load a CodeStorage implementation.
 */
@Slf4j
public class ServiceProviderRegistry {

    private static class NoServiceProvider implements ServiceProvider {

        private static final NoServiceProvider INSTANCE = new NoServiceProvider();

        @Override
        public CompletionsService getCompletionsService(Map<String, Object> map) throws Exception {
            throw new IllegalArgumentException("No AI ServiceProvider found for resource " + map);
        }

        @Override
        public EmbeddingsService getEmbeddingsService(Map<String, Object> map) throws Exception {
            throw new IllegalArgumentException("No AI ServiceProvider found for resource " + map);
        }

        @Override
        public void close() {
        }
    }

    public static ServiceProvider getServiceProvider(Map<String, Object> agentConfiguration) {
        if (agentConfiguration == null || agentConfiguration.isEmpty()) {
            return null;
        }
        if (log.isDebugEnabled()) {
            log.debug("Loading AI ServiceProvider implementation for {}", agentConfiguration);
        }

        ServiceLoader<ServiceProviderProvider> loader = ServiceLoader.load(ServiceProviderProvider.class);
        Optional<ServiceLoader.Provider<ServiceProviderProvider>> provider = loader
                .stream()
                .filter(p -> {
                    return p.get().supports(agentConfiguration);
                })
                .findFirst();
        if (provider.isPresent()) {
            return provider.get().get().createImplementation(agentConfiguration);
        } else {
            return NoServiceProvider.INSTANCE;
        }
    }


}
