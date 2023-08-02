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
package com.datastax.oss.sga.api.runtime;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.Pipeline;

public interface AgentNodeProvider {

    /**
     * Create an Implementation of an Agent that can be deployed on the give runtimes.
     * @param agentConfiguration
     * @param module
     * @param physicalApplicationInstance
     * @param clusterRuntime
     * @param pluginsRegistry
     * @param streamingClusterRuntime
     * @return the Agent
     */
    AgentNode createImplementation(AgentConfiguration agentConfiguration,
                                   Module module,
                                   Pipeline pipeline,
                                   ExecutionPlan physicalApplicationInstance,
                                   ComputeClusterRuntime clusterRuntime,
                                   PluginsRegistry pluginsRegistry,
                                   StreamingClusterRuntime streamingClusterRuntime);

    /**
     * Returns the ability of an Agent to be deployed on the give runtimes.
     * @param type
     * @param clusterRuntime
     * @return true if this provider that can create the implementation
     */
    boolean supports(String type, ComputeClusterRuntime clusterRuntime);

}
