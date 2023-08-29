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
package ai.langstream.api.runtime;

import ai.langstream.api.model.ComputeCluster;
import ai.langstream.api.model.StreamingCluster;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The runtime registry is a singleton that holds all the runtime information about the possible
 * implementations of the LangStream API.
 */
public class ClusterRuntimeRegistry implements AutoCloseable {

    private final Map<String, Map<String, Object>> computeClusterConfigurations;
    protected final Map<String, ComputeClusterRuntime> computeClusterImplementations =
            new ConcurrentHashMap<>();
    protected final Map<String, StreamingClusterRuntime> streamingClusterImplementations =
            new ConcurrentHashMap<>();

    public ClusterRuntimeRegistry() {
        this(Collections.emptyMap());
    }

    public ClusterRuntimeRegistry(Map<String, Map<String, Object>> computeClusterConfigurations) {
        this.computeClusterConfigurations =
                Collections.unmodifiableMap(computeClusterConfigurations);
    }

    public ComputeClusterRuntime getClusterRuntime(ComputeCluster computeCluster) {
        Objects.requireNonNull(computeCluster, "computeCluster cannot be null");
        Objects.requireNonNull(computeCluster.type(), "computeCluster type cannot be null");

        final Map<String, Object> configuration =
                computeClusterConfigurations.getOrDefault(
                        computeCluster.type(), Collections.emptyMap());

        return computeClusterImplementations.computeIfAbsent(
                computeCluster.type(),
                c -> ClusterRuntimeRegistry.loadClusterRuntime(c, configuration));
    }

    public StreamingClusterRuntime getStreamingClusterRuntime(StreamingCluster streamingCluster) {
        Objects.requireNonNull(streamingCluster, "streamingCluster cannot be null");
        Objects.requireNonNull(streamingCluster.type(), "streamingCluster type cannot be null");
        return streamingClusterImplementations.computeIfAbsent(
                streamingCluster.type(), ClusterRuntimeRegistry::loadStreamingClusterRuntime);
    }

    private static ComputeClusterRuntime loadClusterRuntime(
            String clusterType, Map<String, Object> clusterRuntimeConfiguration) {
        ServiceLoader<ComputeClusterRuntimeProvider> loader =
                ServiceLoader.load(ComputeClusterRuntimeProvider.class);
        ServiceLoader.Provider<ComputeClusterRuntimeProvider> clusterRuntimeProviderProvider =
                loader.stream()
                        .filter(p -> p.get().supports(clusterType))
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new RuntimeException(
                                                "No ClusterRuntimeProvider found for type "
                                                        + clusterType));

        final ComputeClusterRuntime implementation =
                clusterRuntimeProviderProvider.get().getImplementation();
        implementation.initialize(clusterRuntimeConfiguration);
        return implementation;
    }

    private static StreamingClusterRuntime loadStreamingClusterRuntime(
            String streamingClusterType) {
        ServiceLoader<StreamingClusterRuntimeProvider> loader =
                ServiceLoader.load(StreamingClusterRuntimeProvider.class);
        ServiceLoader.Provider<StreamingClusterRuntimeProvider> clusterRuntimeProviderProvider =
                loader.stream()
                        .filter(p -> p.get().supports(streamingClusterType))
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new RuntimeException(
                                                "No StreamingClusterRuntimeProvider found for type "
                                                        + streamingClusterType));

        return clusterRuntimeProviderProvider.get().getImplementation();
    }

    @Override
    public void close() {
        for (ComputeClusterRuntime computeClusterRuntime : computeClusterImplementations.values()) {
            computeClusterRuntime.close();
        }
        for (StreamingClusterRuntime streamingClusterRuntime :
                streamingClusterImplementations.values()) {
            streamingClusterRuntime.close();
        }
    }
}
