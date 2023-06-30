package com.datastax.oss.sga.api.runtime;

import com.datastax.oss.sga.api.model.StreamingCluster;

import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The runtime registry is a singleton that holds all the runtime information about the
 * possible implementations of the SGA API.
 */
public class ClusterRuntimeRegistry {

    private final Map<String, ClusterRuntime<?>> registry = new ConcurrentHashMap<>();

    public ClusterRuntime getClusterRuntime(StreamingCluster streamingCluster) {
        Objects.requireNonNull(streamingCluster);
        Objects.requireNonNull(streamingCluster.type());
        return registry.computeIfAbsent(streamingCluster.type(), ClusterRuntimeRegistry::loadClusterRuntime);
    }

    private static ClusterRuntime<?> loadClusterRuntime(String streamingClusterType) {
        ServiceLoader<ClusterRuntimeProvider> loader = ServiceLoader.load(ClusterRuntimeProvider.class);
        ServiceLoader.Provider<ClusterRuntimeProvider> clusterRuntimeProviderProvider = loader
                .stream()
                .filter(p -> {
                    return p.get().supports(streamingClusterType);
                })
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No ClusterRuntimeProvider found for type " + streamingClusterType));

        return clusterRuntimeProviderProvider.get().getImplementation();
    }

}
