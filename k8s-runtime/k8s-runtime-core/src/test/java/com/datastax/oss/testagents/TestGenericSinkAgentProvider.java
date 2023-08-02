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
package com.datastax.oss.testagents;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.Pipeline;
import com.datastax.oss.sga.api.runtime.ComponentType;
import com.datastax.oss.sga.api.runtime.ComputeClusterRuntime;
import com.datastax.oss.sga.api.runtime.ConnectionImplementation;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.Topic;
import com.datastax.oss.sga.impl.common.AbstractAgentProvider;

import com.datastax.oss.sga.runtime.impl.k8s.KubernetesClusterRuntime;
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
    protected Map<String, Object> computeAgentConfiguration(AgentConfiguration agentConfiguration, Module module, Pipeline pipeline,  ExecutionPlan physicalApplicationInstance, ComputeClusterRuntime clusterRuntime) {
        Map<String, Object> copy = super.computeAgentConfiguration(agentConfiguration, module, pipeline, physicalApplicationInstance, clusterRuntime);

        // we can auto-wire the "topics" configuration property
        ConnectionImplementation connectionImplementation = physicalApplicationInstance.getConnectionImplementation(module, agentConfiguration.getInput());
        if (connectionImplementation instanceof Topic topic) {
            copy.put("topics", topic.topicName());
        }
        return copy;
    }
}

