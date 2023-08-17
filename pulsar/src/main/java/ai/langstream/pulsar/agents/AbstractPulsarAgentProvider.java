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
package ai.langstream.pulsar.agents;

import ai.langstream.pulsar.PulsarClientUtils;
import ai.langstream.pulsar.PulsarClusterRuntimeConfiguration;
import ai.langstream.pulsar.PulsarName;
import ai.langstream.api.model.AgentConfiguration;
import ai.langstream.api.runtime.AgentNodeMetadata;
import ai.langstream.api.runtime.ComponentType;
import ai.langstream.api.runtime.ComputeClusterRuntime;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.StreamingClusterRuntime;
import ai.langstream.impl.common.AbstractAgentProvider;

import java.util.List;
import java.util.Set;

public abstract class AbstractPulsarAgentProvider extends AbstractAgentProvider {

    public AbstractPulsarAgentProvider(Set<String> supportedTypes,
                                       List<String> supportedClusterTypes) {
        super(supportedTypes, supportedClusterTypes);
    }

    public static PulsarName computePulsarName(ExecutionPlan instance, AgentConfiguration agentConfiguration) {
        PulsarClusterRuntimeConfiguration pulsarClusterRuntimeConfiguration = PulsarClientUtils.getPulsarClusterRuntimeConfiguration(instance.getApplication().getInstance().streamingCluster());
        return new PulsarName(pulsarClusterRuntimeConfiguration.getDefaultTenant(),
                pulsarClusterRuntimeConfiguration.getDefaultNamespace(), sanitizeName(agentConfiguration));
    }

    private static String sanitizeName(AgentConfiguration agentConfiguration) {
        return agentConfiguration.getName()
                .replace(" ", "-")
                .replace(".", "-")
                .toLowerCase();
    }

    @Override
    protected AgentNodeMetadata computeAgentMetadata(AgentConfiguration agentConfiguration, ExecutionPlan physicalApplicationInstance, ComputeClusterRuntime clusterRuntime,
                                                     StreamingClusterRuntime streamingClusterRuntime) {
        ComponentType componentType = getComponentType(agentConfiguration);
        String agentType = getAgentType(agentConfiguration);
        return computePulsarMetadata(agentConfiguration, physicalApplicationInstance, componentType, agentType);
    }

    public static PulsarAgentNodeMetadata computePulsarMetadata(AgentConfiguration agentConfiguration, ExecutionPlan physicalApplicationInstance,
                                                                ComponentType componentType, String agentType) {
        PulsarName pulsarName = computePulsarName(physicalApplicationInstance, agentConfiguration);
        return new PulsarAgentNodeMetadata(pulsarName, componentType, agentType);
    }
}
