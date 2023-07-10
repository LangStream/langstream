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
package com.datastax.oss.sga.api.runtime;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.model.ApplicationInstance;

/**
 * This is the interface that the SGA framework uses to interact with the cluster. It is used to
 * model a physical cluster runtime (Pulsar, Kafka....)
 */
public interface ClusterRuntime<T extends PhysicalApplicationInstance> {

    String getClusterType();

    /**
     * Create a physical application instance from the logical application instance.
     *
     * @param applicationInstance
     * @param streamingClusterRuntime
     * @return the physical application instance
     */
    T createImplementation(ApplicationInstance applicationInstance, PluginsRegistry pluginsRegistry, StreamingClusterRuntime streamingClusterRuntime);

    void deploy(ApplicationInstance logicalInstance, T applicationInstance, StreamingClusterRuntime streamingClusterRuntime);

    void delete(ApplicationInstance logicalInstance, T applicationInstance, StreamingClusterRuntime streamingClusterRuntime);

    default Object computeAgentMetadata(AgentConfiguration agentConfiguration, PhysicalApplicationInstance physicalApplicationInstance) {
        return null;
    }
}
