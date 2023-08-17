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

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.Pipeline;
import com.datastax.oss.sga.api.runtime.ComponentType;
import com.datastax.oss.sga.api.runtime.ComputeClusterRuntime;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import ai.langstream.pulsar.PulsarClusterRuntime;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class GenericPulsarSourceAgentProvider extends AbstractPulsarAgentProvider {

    public GenericPulsarSourceAgentProvider() {
        super(Set.of("source"), List.of(PulsarClusterRuntime.CLUSTER_TYPE));
    }

    @Override
    protected String getAgentType(AgentConfiguration configuration) {
        String sourceType = (String)configuration.getConfiguration()
                .get("sourceType");
        if (sourceType == null) {
            throw new IllegalArgumentException("For the generic pulsar-source you must configure the sourceType configuration property");
        }
        return sourceType;
    }

    @Override
    protected ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        return ComponentType.SOURCE;
    }

    @Override
    protected Map<String, Object> computeAgentConfiguration(AgentConfiguration agentConfiguration, Module module, Pipeline pipeline, ExecutionPlan physicalApplicationInstance, ComputeClusterRuntime clusterRuntime) {
        Map<String, Object> copy = super.computeAgentConfiguration(agentConfiguration, module, pipeline, physicalApplicationInstance, clusterRuntime);
        copy.remove("sourceType");
        return copy;
    }
}
