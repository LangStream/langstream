package com.datastax.oss.sga.api.runtime;

import com.datastax.oss.sga.api.model.ComputeCluster;
import com.datastax.oss.sga.api.model.StreamingCluster;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The runtime registry is a singleton that holds all the runtime information about the
 * possible implementations of the SGA API.
 */
public class ClusterRuntimeRegistry {

    private final Map<String, Map<String, Object>> computeClusterConfigurations;

    public ClusterRuntimeRegistry() {
        this(Collections.emptyMap());
    }
    public ClusterRuntimeRegistry(Map<String, Map<String, Object>> computeClusterConfigurations) {
        this.computeClusterConfigurations = Collections.unmodifiableMap(computeClusterConfigurations);
    }

    protected final Map<String, ComputeClusterRuntime> computeClusterImplementations = new ConcurrentHashMap<>();
    protected final Map<String, StreamingClusterRuntime> streamingClusterImplementations = new ConcurrentHashMap<>();

    public ComputeClusterRuntime getClusterRuntime(ComputeCluster computeCluster) {
        Objects.requireNonNull(computeCluster, "computeCluster cannot be null");
        Objects.requireNonNull(computeCluster.type(), "computeCluster type cannot be null");

        final Map<String, Object> configuration = computeClusterConfigurations.getOrDefault(computeCluster.type(),
                Collections.emptyMap());

        return computeClusterImplementations.computeIfAbsent(computeCluster.type(), c -> ClusterRuntimeRegistry
                .loadClusterRuntime(c, configuration));
    }

    public StreamingClusterRuntime getStreamingClusterRuntime(StreamingCluster streamingCluster) {
        Objects.requireNonNull(streamingCluster, "streamingCluster cannot be null");
        Objects.requireNonNull(streamingCluster.type(), "streamingCluster type cannot be null");
        return streamingClusterImplementations.computeIfAbsent(streamingCluster.type(), ClusterRuntimeRegistry::loadStreamingClusterRuntime);
    }

    private static ComputeClusterRuntime loadClusterRuntime(String clusterType,
                                                            Map<String, Object> clusterRuntimeConfiguration) {
        ServiceLoader<ComputeClusterRuntimeProvider> loader = ServiceLoader.load(ComputeClusterRuntimeProvider.class);
        ServiceLoader.Provider<ComputeClusterRuntimeProvider> clusterRuntimeProviderProvider = loader
                .stream()
                .filter(p -> {
                    return p.get().supports(clusterType);
                })
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No ClusterRuntimeProvider found for type " + clusterType));

        final ComputeClusterRuntime implementation = clusterRuntimeProviderProvider.get().getImplementation();
        implementation.initialize(clusterRuntimeConfiguration);
        return implementation;
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
