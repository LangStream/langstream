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
package ai.langstream.runtime.impl.testagents;

import ai.langstream.api.model.AgentConfiguration;
import ai.langstream.api.model.Module;
import ai.langstream.api.model.Pipeline;
import ai.langstream.api.runtime.ComponentType;
import ai.langstream.api.runtime.ComputeClusterRuntime;
import ai.langstream.api.runtime.ConnectionImplementation;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.Topic;
import ai.langstream.impl.common.AbstractAgentProvider;

import ai.langstream.runtime.impl.k8s.KubernetesClusterRuntime;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestGenericSinkAgentProvider extends AbstractAgentProvider {

    public TestGenericSinkAgentProvider() {
        super(Set.of("sink"), List.of(KubernetesClusterRuntime.CLUSTER_TYPE, "none"));
    }

    @Override
    protected ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        return ComponentType.SINK;
    }

    @Override
    protected Map<String, Object> computeAgentConfiguration(AgentConfiguration agentConfiguration, Module module, Pipeline pipeline, ExecutionPlan executionPlan, ComputeClusterRuntime clusterRuntime) {
        Map<String, Object> copy = super.computeAgentConfiguration(agentConfiguration, module, pipeline, executionPlan, clusterRuntime);

        // we can auto-wire the "topics" configuration property
        ConnectionImplementation connectionImplementation = executionPlan.getConnectionImplementation(module, agentConfiguration.getInput());
        if (connectionImplementation instanceof Topic topic) {
            copy.put("topics", topic.topicName());
        }
        return copy;
    }
}

