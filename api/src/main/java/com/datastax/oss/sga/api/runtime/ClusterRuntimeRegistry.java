package com.datastax.oss.sga.api.runtime;

import com.datastax.oss.sga.api.model.ComputeCluster;
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

    protected final Map<String, ClusterRuntime> computeClusterImplementations = new ConcurrentHashMap<>();
    protected final Map<String, StreamingClusterRuntime> streamingClusterImplementations = new ConcurrentHashMap<>();

    public ClusterRuntime getClusterRuntime(ComputeCluster computeCluster) {
        Objects.requireNonNull(computeCluster, "computeCluster cannot be null");
        Objects.requireNonNull(computeCluster.type(), "computeCluster type cannot be null");
        return computeClusterImplementations.computeIfAbsent(computeCluster.type(), ClusterRuntimeRegistry::loadClusterRuntime);
    }

    public StreamingClusterRuntime getStreamingClusterRuntime(StreamingCluster streamingCluster) {
        Objects.requireNonNull(streamingCluster, "streamingCluster cannot be null");
        Objects.requireNonNull(streamingCluster.type(), "streamingCluster type cannot be null");
        return streamingClusterImplementations.computeIfAbsent(streamingCluster.type(), ClusterRuntimeRegistry::loadStreamingClusterRuntime);
    }

    private static ClusterRuntime loadClusterRuntime(String clusterType) {
        ServiceLoader<ClusterRuntimeProvider> loader = ServiceLoader.load(ClusterRuntimeProvider.class);
        ServiceLoader.Provider<ClusterRuntimeProvider> clusterRuntimeProviderProvider = loader
                .stream()
                .filter(p -> {
                    return p.get().supports(clusterType);
                })
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No ClusterRuntimeProvider found for type " + clusterType));

        return clusterRuntimeProviderProvider.get().getImplementation();
    }

    private static StreamingClusterRuntime loadStreamingClusterRuntime(String streamingClusterType) {
        ServiceLoader<StreamingClusterRuntimeProvider> loader = ServiceLoader.load(StreamingClusterRuntimeProvider.class);
        ServiceLoader.Provider<StreamingClusterRuntimeProvider> clusterRuntimeProviderProvider = loader
                .stream()
                .filter(p -> {
                    return p.get().supports(streamingClusterType);
                })
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No StreamingClusterRuntimeProvider found for type " + streamingClusterType));

        return clusterRuntimeProviderProvider.get().getImplementation();
    }

}
