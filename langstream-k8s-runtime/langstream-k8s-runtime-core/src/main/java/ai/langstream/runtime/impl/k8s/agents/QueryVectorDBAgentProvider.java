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
package ai.langstream.runtime.impl.k8s.agents;

import ai.langstream.api.model.AgentConfiguration;
import ai.langstream.api.model.Application;
import ai.langstream.api.model.Module;
import ai.langstream.api.model.Pipeline;
import ai.langstream.api.model.Resource;
import ai.langstream.api.runtime.ComponentType;
import ai.langstream.api.runtime.ComputeClusterRuntime;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.impl.agents.AbstractComposableAgentProvider;
import ai.langstream.runtime.impl.k8s.KubernetesClusterRuntime;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class QueryVectorDBAgentProvider extends AbstractComposableAgentProvider {

    public QueryVectorDBAgentProvider() {
        super(Set.of("query-vector-db", "vector-db-sink"), List.of(KubernetesClusterRuntime.CLUSTER_TYPE));
    }

    @Override
    protected ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        switch (agentConfiguration.getType()) {
            case "query-vector-db":
                return ComponentType.PROCESSOR;
            case "vector-db-sink":
                return ComponentType.SINK;
            default:
                throw new IllegalStateException();
        }
    }

    @Override
    protected Map<String, Object> computeAgentConfiguration(AgentConfiguration agentConfiguration, Module module,
                                                            Pipeline pipeline, ExecutionPlan executionPlan, ComputeClusterRuntime clusterRuntime) {
        Map<String, Object> originalConfiguration = super.computeAgentConfiguration(agentConfiguration, module, pipeline, executionPlan, clusterRuntime);

        // get the datasource configuration and inject it into the agent configuration
        String resourceId = (String) originalConfiguration.remove("datasource");
        if (resourceId == null || resourceId.isEmpty()) {
            throw new IllegalArgumentException(
                    "Missing required field 'datasource' in agent definition, type=" + agentConfiguration.getType()
                            + ", name=" + agentConfiguration.getName() + ", id=" + agentConfiguration.getId());
        }
        generateDataSourceConfiguration(resourceId, executionPlan.getApplication(), originalConfiguration);

        return originalConfiguration;
    }

    private void generateDataSourceConfiguration(String resourceId, Application applicationInstance, Map<String, Object> configuration) {

        Resource resource = applicationInstance.getResources().get(resourceId);
        log.info("Generating datasource configuration for {}", resourceId);
        if (resource != null) {
            if (!resource.type().equals("datasource")
                    && !resource.type().equals("vector-database")
                ) {
                throw new IllegalArgumentException("Resource " + resourceId + " is not type=datasource or type=vector-database");
            }
            if (configuration.containsKey("datasource")) {
                throw new IllegalArgumentException("Only one datasource is supported");
            }
            configuration.put("datasource", new HashMap<>(resource.configuration()));
        } else {
            throw new IllegalArgumentException("Resource " + resourceId + " not found");
        }
    }
}
