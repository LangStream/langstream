/**
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
