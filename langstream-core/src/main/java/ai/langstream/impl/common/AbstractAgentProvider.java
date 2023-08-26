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
package ai.langstream.impl.common;

import ai.langstream.api.model.AgentConfiguration;
import ai.langstream.api.model.Module;
import ai.langstream.api.model.Pipeline;
import ai.langstream.api.runtime.AgentNode;
import ai.langstream.api.runtime.AgentNodeMetadata;
import ai.langstream.api.runtime.AgentNodeProvider;
import ai.langstream.api.runtime.ComponentType;
import ai.langstream.api.runtime.ComputeClusterRuntime;
import ai.langstream.api.runtime.ConnectionImplementation;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.api.runtime.StreamingClusterRuntime;
import lombok.Getter;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Getter
public abstract class AbstractAgentProvider implements AgentNodeProvider {

    protected final Set<String> supportedTypes;
    protected final List<String> supportedClusterTypes;

    public AbstractAgentProvider(Set<String> supportedTypes, List<String> supportedClusterTypes) {
        this.supportedTypes = Collections.unmodifiableSet(supportedTypes);
        this.supportedClusterTypes = Collections.unmodifiableList(supportedClusterTypes);
    }

    protected boolean isComposable(AgentConfiguration agentConfiguration) {
        return false;
    }

    protected ConnectionImplementation computeInput(AgentConfiguration agentConfiguration,
                                                    Module module,
                                                    Pipeline pipeline,
                                                    ExecutionPlan physicalApplicationInstance,
                                                    ComputeClusterRuntime clusterRuntime, StreamingClusterRuntime streamingClusterRuntime) {
        if (agentConfiguration.getInput() != null) {
            return clusterRuntime
                    .getConnectionImplementation(module, pipeline, agentConfiguration.getInput(), ConnectionImplementation.ConnectionDirection.INPUT, physicalApplicationInstance, streamingClusterRuntime);
        } else {
            return null;
        }
    }

    protected ConnectionImplementation computeOutput(AgentConfiguration agentConfiguration,
                                                     Module module,
                                                     Pipeline pipeline,
                                                     ExecutionPlan physicalApplicationInstance,
                                                     ComputeClusterRuntime clusterRuntime, StreamingClusterRuntime streamingClusterRuntime) {
        if (agentConfiguration.getOutput() != null) {
            return clusterRuntime
                    .getConnectionImplementation(module, pipeline, agentConfiguration.getOutput(), ConnectionImplementation.ConnectionDirection.OUTPUT, physicalApplicationInstance, streamingClusterRuntime);
        } else {
            return null;
        }
    }

    /**
     * Allow to override the component type
     * @param agentConfiguration the agent configuration
     * @return the component type
     */
    protected abstract ComponentType getComponentType(AgentConfiguration agentConfiguration);

    /**
     * Allow to override the agent type
     * @param agentConfiguration the agent configuration
     * @return the agent type
     */
    protected String getAgentType(AgentConfiguration agentConfiguration) {
        return agentConfiguration.getType();
    }

    protected AgentNodeMetadata computeAgentMetadata(AgentConfiguration agentConfiguration,
                                                     ExecutionPlan physicalApplicationInstance,
                                                     ComputeClusterRuntime clusterRuntime,
                                                     StreamingClusterRuntime streamingClusterRuntime) {
        return clusterRuntime.computeAgentMetadata(agentConfiguration, physicalApplicationInstance, streamingClusterRuntime);
    }

    protected Map<String, Object> computeAgentConfiguration(AgentConfiguration agentConfiguration, Module module,
                                          Pipeline pipeline,
                                          ExecutionPlan executionPlan,
                                          ComputeClusterRuntime clusterRuntime) {
        return new HashMap<>(agentConfiguration.getConfiguration());
    }

    @Override
    public AgentNode createImplementation(AgentConfiguration agentConfiguration,
                                          Module module,
                                          Pipeline pipeline,
                                          ExecutionPlan physicalApplicationInstance,
                                          ComputeClusterRuntime clusterRuntime, PluginsRegistry pluginsRegistry,
                                          StreamingClusterRuntime streamingClusterRuntime) {
        Object metadata = computeAgentMetadata(agentConfiguration, physicalApplicationInstance, clusterRuntime, streamingClusterRuntime);
        String agentType = getAgentType(agentConfiguration);
        ComponentType componentType = getComponentType(agentConfiguration);
        Map<String, Object> configuration = computeAgentConfiguration(agentConfiguration, module, pipeline,
                physicalApplicationInstance, clusterRuntime);
        // we create the output connection first to make sure that the topic is created
        ConnectionImplementation output = computeOutput(agentConfiguration, module, pipeline, physicalApplicationInstance, clusterRuntime, streamingClusterRuntime);
        ConnectionImplementation input = computeInput(agentConfiguration, module, pipeline, physicalApplicationInstance, clusterRuntime, streamingClusterRuntime);
        boolean composable = isComposable(agentConfiguration);
        return new DefaultAgentNode(agentConfiguration.getId(),
                agentType,
                componentType,
                configuration,
                composable,
                metadata, input, output,
                agentConfiguration.getResources(),
                agentConfiguration.getErrors());
    }

    @Override
    public boolean supports(String type, ComputeClusterRuntime clusterRuntime) {
        return supportedTypes.contains(type) && supportedClusterTypes.contains(clusterRuntime.getClusterType());
    }
}
