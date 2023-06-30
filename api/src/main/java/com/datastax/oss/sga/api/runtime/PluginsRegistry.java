package com.datastax.oss.sga.api.runtime;

import lombok.extern.slf4j.Slf4j;

import java.util.ServiceLoader;

@Slf4j
public class PluginsRegistry {
    public AgentImplementationProvider lookupAgentImplementation(String type, ClusterRuntime<?> clusterRuntime) {
        log.info("Looking for an implementation of agent type {} on {}", type, clusterRuntime.getClusterType());
        ServiceLoader<AgentImplementationProvider> loader = ServiceLoader.load(AgentImplementationProvider.class);
        ServiceLoader.Provider<AgentImplementationProvider> agentRuntimeProviderProvider = loader
                .stream()
                .filter(p -> {
                    AgentImplementationProvider agentImplementationProvider = p.get();
                    boolean success = agentImplementationProvider.supports(type, clusterRuntime);
                    log.info("Tested {}: result {}", agentImplementationProvider, success);
                    return success;
                })
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No AgentImplementationProvider found for type " + type
                        + " for cluster type "+clusterRuntime.getClusterType()));
        return agentRuntimeProviderProvider.get();
    }

}
