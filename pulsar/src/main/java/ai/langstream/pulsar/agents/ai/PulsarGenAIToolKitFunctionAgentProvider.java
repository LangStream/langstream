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
package ai.langstream.pulsar.agents.ai;

import ai.langstream.pulsar.PulsarClusterRuntime;
import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.runtime.AgentNodeMetadata;
import com.datastax.oss.sga.api.runtime.ComponentType;
import com.datastax.oss.sga.api.runtime.ComputeClusterRuntime;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.StreamingClusterRuntime;
import com.datastax.oss.sga.impl.agents.ai.GenAIToolKitFunctionAgentProvider;
import ai.langstream.pulsar.agents.AbstractPulsarAgentProvider;

public class PulsarGenAIToolKitFunctionAgentProvider extends GenAIToolKitFunctionAgentProvider {

    public PulsarGenAIToolKitFunctionAgentProvider() {
        super(PulsarClusterRuntime.CLUSTER_TYPE);
    }

    @Override
    protected boolean isComposable(AgentConfiguration agentConfiguration) {
        return false;
    }

    @Override
    protected String getAgentType(AgentConfiguration agentConfiguration) {
        // all the agents in the AT ToolKit can be generated from one single agent implementation
        // this is important because the runtime is able to "merge" agents of the same type
        return "ai-tools";
    }

    @Override
    protected AgentNodeMetadata computeAgentMetadata(AgentConfiguration agentConfiguration,
                                                     ExecutionPlan physicalApplicationInstance,
                                                     ComputeClusterRuntime clusterRuntime,
                                                     StreamingClusterRuntime streamingClusterRuntime) {
        ComponentType componentType = getComponentType(agentConfiguration);
        String agentType = getAgentType(agentConfiguration);
        return AbstractPulsarAgentProvider.computePulsarMetadata(agentConfiguration, physicalApplicationInstance, componentType, agentType);
    }
}
