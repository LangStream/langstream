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

import ai.langstream.api.model.AgentConfiguration;
import ai.langstream.api.model.Application;
import ai.langstream.api.model.Connection;
import ai.langstream.api.model.Module;
import ai.langstream.api.model.Pipeline;
import ai.langstream.api.model.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is the interface that the LangStream framework uses to interact with the cluster. It is used
 * to model a physical cluster runtime (Pulsar, Kafka....)
 */
public interface ComputeClusterRuntime extends AutoCloseable {

    String getClusterType();

    void initialize(Map<String, Object> configuration);

    /**
     * Create a physical application instance from the logical application instance.
     *
     * @param application the logical application instance
     * @param streamingClusterRuntime the cluster runtime
     * @return the physical application instance
     */
    ExecutionPlan buildExecutionPlan(
            String applicationId,
            Application application,
            PluginsRegistry pluginsRegistry,
            StreamingClusterRuntime streamingClusterRuntime);

    /**
     * Get a connection implementation.
     *
     * @param module the module
     * @param connection the connection
     * @return the connection implementation
     */
    ConnectionImplementation getConnectionImplementation(
            Module module,
            Pipeline pipeline,
            Connection connection,
            ConnectionImplementation.ConnectionDirection direction,
            ExecutionPlan applicationInstance,
            StreamingClusterRuntime streamingClusterRuntime);

    Object deploy(
            String tenant,
            ExecutionPlan applicationInstance,
            StreamingClusterRuntime streamingClusterRuntime,
            String codeStorageArchiveId,
            DeployContext deployContext);

    void delete(
            String tenant,
            ExecutionPlan applicationInstance,
            StreamingClusterRuntime streamingClusterRuntime,
            String codeStorageArchiveId,
            DeployContext deployContext);

    List<ExecutionPlanOptimiser> getExecutionPlanOptimisers();

    default AgentNodeMetadata computeAgentMetadata(
            AgentConfiguration agentConfiguration,
            ExecutionPlan physicalApplicationInstance,
            StreamingClusterRuntime streamingClusterRuntime) {
        return null;
    }

    default Map<String, Object> getResourceImplementation(
            Resource resource, PluginsRegistry pluginsRegistry) {
        return new HashMap<>(resource.configuration());
    }

    default void close() {}
}
