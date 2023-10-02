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
package ai.langstream.runtime.impl.k8s.agents;

import ai.langstream.api.doc.AgentConfig;
import ai.langstream.api.doc.ConfigProperty;
import ai.langstream.api.model.AgentConfiguration;
import ai.langstream.api.model.Application;
import ai.langstream.api.model.Module;
import ai.langstream.api.model.Pipeline;
import ai.langstream.api.model.Resource;
import ai.langstream.api.runtime.ComponentType;
import ai.langstream.api.runtime.ComputeClusterRuntime;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.impl.agents.AbstractComposableAgentProvider;
import ai.langstream.impl.agents.ai.steps.QueryConfiguration;
import ai.langstream.runtime.impl.k8s.KubernetesClusterRuntime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryVectorDBAgentProvider extends AbstractComposableAgentProvider {

    protected static final String QUERY_VECTOR_DB = "query-vector-db";
    protected static final String VECTOR_DB_SINK = "vector-db-sink";

    public QueryVectorDBAgentProvider() {
        super(
                Set.of(QUERY_VECTOR_DB, VECTOR_DB_SINK),
                List.of(KubernetesClusterRuntime.CLUSTER_TYPE, "none"));
    }

    @Override
    protected ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        return switch (agentConfiguration.getType()) {
            case QUERY_VECTOR_DB -> ComponentType.PROCESSOR;
            case VECTOR_DB_SINK -> ComponentType.SINK;
            default -> throw new IllegalStateException();
        };
    }

    @Override
    protected Map<String, Object> computeAgentConfiguration(
            AgentConfiguration agentConfiguration,
            Module module,
            Pipeline pipeline,
            ExecutionPlan executionPlan,
            ComputeClusterRuntime clusterRuntime,
            PluginsRegistry pluginsRegistry) {
        Map<String, Object> originalConfiguration =
                super.computeAgentConfiguration(
                        agentConfiguration,
                        module,
                        pipeline,
                        executionPlan,
                        clusterRuntime,
                        pluginsRegistry);

        // get the datasource configuration and inject it into the agent configuration
        String resourceId = (String) originalConfiguration.remove("datasource");
        if (resourceId == null) {
            throw new IllegalStateException(
                    "datasource is required but this exception should have been raised before ?");
        }
        generateDataSourceConfiguration(
                resourceId,
                executionPlan.getApplication(),
                originalConfiguration,
                clusterRuntime,
                pluginsRegistry);

        return originalConfiguration;
    }

    private void generateDataSourceConfiguration(
            String resourceId,
            Application applicationInstance,
            Map<String, Object> configuration,
            ComputeClusterRuntime computeClusterRuntime,
            PluginsRegistry pluginsRegistry) {

        Resource resource = applicationInstance.getResources().get(resourceId);
        log.info("Generating datasource configuration for {}", resourceId);
        if (resource != null) {
            Map<String, Object> resourceImplementation =
                    computeClusterRuntime.getResourceImplementation(resource, pluginsRegistry);
            if (!resource.type().equals("datasource")
                    && !resource.type().equals("vector-database")) {
                throw new IllegalArgumentException(
                        "Resource '"
                                + resourceId
                                + "' is not type=datasource or type=vector-database");
            }
            if (configuration.containsKey("datasource")) {
                throw new IllegalArgumentException("Only one datasource is supported");
            }
            configuration.put("datasource", resourceImplementation);
        } else {
            throw new IllegalArgumentException("Resource '" + resourceId + "' not found");
        }
    }

    @Override
    protected Class getAgentConfigModelClass(String type) {
        return switch (type) {
            case QUERY_VECTOR_DB -> QueryVectorDBConfig.class;
            case VECTOR_DB_SINK -> VectorDBSinkConfig.class;
            default -> throw new IllegalStateException(type);
        };
    }

    @Override
    protected boolean isAgentConfigModelAllowUnknownProperties(String type) {
        return switch (type) {
            case QUERY_VECTOR_DB -> false;
            case VECTOR_DB_SINK -> true;
            default -> throw new IllegalStateException(type);
        };
    }

    @AgentConfig(
            name = "Query a vector database",
            description =
                    """
            Query a vector database using Vector Search capabilities.
            """)
    @Data
    public static class QueryVectorDBConfig extends QueryConfiguration {}

    @AgentConfig(
            name = "Vector database sink",
            description =
                    """
            Store vectors in a vector database.
            Configuration properties depends on the vector database implementation, specified by the "datasource" property.
            """)
    @Data
    public static class VectorDBSinkConfig {
        @ConfigProperty(
                description =
                        """
                        The defined datasource ID to use to store the vectors.
                                """,
                required = true)
        private String datasource;
    }
}
