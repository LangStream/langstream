package com.datastax.oss.sga.api.runner.topics;

import com.datastax.oss.sga.api.model.StreamingCluster;

import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The runtime registry is a singleton that holds all the runtime information about the
 * possible implementations of the SGA API.
 */
public class TopicConnectionsRuntimeRegistry {

    protected final Map<String, TopicConnectionsRuntime> implementations = new ConcurrentHashMap<>();

    public TopicConnectionsRuntime getTopicConnectionsRuntime(StreamingCluster streamingCluster) {
        Objects.requireNonNull(streamingCluster, "streamingCluster cannot be null");
        Objects.requireNonNull(streamingCluster.type(), "streamingCluster type cannot be null");
        return implementations.computeIfAbsent(streamingCluster.type(), TopicConnectionsRuntimeRegistry::loadClusterRuntime);
    }

    private static TopicConnectionsRuntime loadClusterRuntime(String streamingClusterType) {
        ServiceLoader<TopicConnectionsRuntimeProvider> loader = ServiceLoader.load(TopicConnectionsRuntimeProvider.class);
        ServiceLoader.Provider<TopicConnectionsRuntimeProvider> clusterRuntimeProviderProvider = loader
                .stream()
                .filter(p -> p.get().supports(streamingClusterType))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No TopicConnectionsRuntimeProvider found for type " + streamingClusterType));

        return clusterRuntimeProviderProvider.get().getImplementation();
    }

}
