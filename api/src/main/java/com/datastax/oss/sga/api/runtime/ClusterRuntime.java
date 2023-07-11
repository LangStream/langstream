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
import com.datastax.oss.sga.api.model.Connection;
import com.datastax.oss.sga.api.model.Module;

/**
 * This is the interface that the SGA framework uses to interact with the cluster. It is used to
 * model a physical cluster runtime (Pulsar, Kafka....)
 */
public interface ClusterRuntime {

    String getClusterType();

    /**
     * Create a physical application instance from the logical application instance.
     *
     * @param applicationInstance
     * @param streamingClusterRuntime
     * @return the physical application instance
     */
    PhysicalApplicationInstance createImplementation(ApplicationInstance applicationInstance, PluginsRegistry pluginsRegistry, StreamingClusterRuntime streamingClusterRuntime);


    /**
     * Get a connection
     * @param module
     * @param connection
     * @return the connection implementation
     */
    ConnectionImplementation getConnectionImplementation(Module module, Connection connection, PhysicalApplicationInstance applicationInstance, StreamingClusterRuntime streamingClusterRuntime);

    void deploy(PhysicalApplicationInstance applicationInstance, StreamingClusterRuntime streamingClusterRuntime);

    void delete(PhysicalApplicationInstance applicationInstance, StreamingClusterRuntime streamingClusterRuntime);

    default Object computeAgentMetadata(AgentConfiguration agentConfiguration, PhysicalApplicationInstance physicalApplicationInstance, StreamingClusterRuntime streamingClusterRuntime) {
        return null;
    }
}
