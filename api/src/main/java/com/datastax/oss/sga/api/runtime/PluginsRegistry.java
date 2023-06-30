package com.datastax.oss.sga.api.runtime;

import java.util.ServiceLoader;

public class PluginsRegistry {
    public AgentImplementationProvider lookupAgentImplementation(String type, ClusterRuntime<?> clusterRuntime) {
        ServiceLoader<AgentImplementationProvider> loader = ServiceLoader.load(AgentImplementationProvider.class);
        ServiceLoader.Provider<AgentImplementationProvider> agentRuntimeProviderProvider = loader
                .stream()
                .filter(p -> {
                    return p.get().supports(type, clusterRuntime);
                })
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No AgentImplementationProvider found for type " + type
                        + " for cluster type "+clusterRuntime.getClusterType()));
        return agentRuntimeProviderProvider.get();
    }

}
