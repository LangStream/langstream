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
import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.Pipeline;

import java.util.List;
import java.util.Map;

/**
 * This is the interface that the SGA framework uses to interact with the cluster. It is used to
 * model a physical cluster runtime (Pulsar, Kafka....)
 */
public interface ComputeClusterRuntime extends AutoCloseable {

    String getClusterType();

    void initialize(Map<String, Object> configuration);

    /**
     * Create a physical application instance from the logical application instance.
     *
     * @param application
     * @param streamingClusterRuntime
     * @return the physical application instance
     */
    ExecutionPlan buildExecutionPlan(String applicationId, Application application, PluginsRegistry pluginsRegistry, StreamingClusterRuntime streamingClusterRuntime);


    /**
     * Get a connection
     * @param module
     * @param connection
     * @return the connection implementation
     */
    Connection getConnectionImplementation(Module module,
                                           Pipeline pipeline,
                                           com.datastax.oss.sga.api.model.Connection connection,
                                           Connection.ConnectionDirection direction,
                                           ExecutionPlan applicationInstance,
                                           StreamingClusterRuntime streamingClusterRuntime);

    Object deploy(String tenant, ExecutionPlan applicationInstance, StreamingClusterRuntime streamingClusterRuntime,
                  String codeStorageArchiveId);

    void delete(String tenant, ExecutionPlan applicationInstance, StreamingClusterRuntime streamingClusterRuntime,
                String codeStorageArchiveId);

    List<ExecutionPlanOptimiser> getExecutionPlanOptimisers();

    default AgentNodeMetadata computeAgentMetadata(AgentConfiguration agentConfiguration, ExecutionPlan physicalApplicationInstance, StreamingClusterRuntime streamingClusterRuntime) {
        return null;
    }

    default void close() {
    }
}
