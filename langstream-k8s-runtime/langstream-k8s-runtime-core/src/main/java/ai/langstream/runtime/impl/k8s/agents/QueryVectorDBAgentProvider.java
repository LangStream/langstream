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
import ai.langstream.api.doc.AgentConfigurationModel;
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
import ai.langstream.impl.uti.ClassConfigValidator;
import ai.langstream.runtime.impl.k8s.KubernetesClusterRuntime;
import ai.langstream.runtime.impl.k8s.agents.vectors.AstraVectorDBVectorDatabaseWriterConfig;
import ai.langstream.runtime.impl.k8s.agents.vectors.CassandraVectorDatabaseWriterConfig;
import ai.langstream.runtime.impl.k8s.agents.vectors.JDBCVectorDatabaseWriterConfig;
import ai.langstream.runtime.impl.k8s.agents.vectors.MilvusVectorDatabaseWriterConfig;
import ai.langstream.runtime.impl.k8s.agents.vectors.OpenSearchVectorDatabaseWriterConfig;
import ai.langstream.runtime.impl.k8s.agents.vectors.PineconeVectorDatabaseWriterConfig;
import ai.langstream.runtime.impl.k8s.agents.vectors.SolrVectorDatabaseWriterConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryVectorDBAgentProvider extends AbstractComposableAgentProvider {

    protected static final ObjectMapper MAPPER = new ObjectMapper();

    @Getter
    @Setter
    public abstract static class VectorDatabaseWriterConfig {
        @ConfigProperty(
                description =
                        """
                                The defined datasource ID to use to store the vectors.
                                        """,
                required = true)
        String datasource;

        public abstract Class getAgentConfigModelClass();

        public abstract boolean isAgentConfigModelAllowUnknownProperties();
    }

    protected static final String QUERY_VECTOR_DB = "query-vector-db";
    protected static final String VECTOR_DB_SINK = "vector-db-sink";
    protected static final Map<String, VectorDatabaseWriterConfig>
            SUPPORTED_VECTOR_DB_SINK_DATASOURCES =
                    Map.of(
                            "cassandra", CassandraVectorDatabaseWriterConfig.CASSANDRA,
                            "astra", CassandraVectorDatabaseWriterConfig.ASTRA,
                            "jdbc", JDBCVectorDatabaseWriterConfig.INSTANCE,
                            "pinecone", PineconeVectorDatabaseWriterConfig.INSTANCE,
                            "opensearch", OpenSearchVectorDatabaseWriterConfig.INSTANCE,
                            "solr", SolrVectorDatabaseWriterConfig.INSTANCE,
                            "milvus", MilvusVectorDatabaseWriterConfig.INSTANCE,
                            "astra-vector-db", AstraVectorDBVectorDatabaseWriterConfig.INSTANCE);

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
            throw new IllegalArgumentException(
                    ClassConfigValidator.formatErrString(
                            new ClassConfigValidator.AgentEntityRef(agentConfiguration),
                            "datasource",
                            "is required"));
        }
        generateDataSourceConfiguration(
                resourceId,
                executionPlan.getApplication(),
                originalConfiguration,
                clusterRuntime,
                pluginsRegistry,
                agentConfiguration);

        return originalConfiguration;
    }

    private boolean isAgentConfigModelAllowUnknownProperties(String type, String service) {
        switch (type) {
            case QUERY_VECTOR_DB:
                return false;
            case VECTOR_DB_SINK:
                {
                    final VectorDatabaseWriterConfig vectorDatabaseSinkConfig =
                            SUPPORTED_VECTOR_DB_SINK_DATASOURCES.get(service);
                    if (vectorDatabaseSinkConfig == null) {
                        throw new IllegalArgumentException(
                                "Unsupported vector database service: "
                                        + service
                                        + ". Supported services are: "
                                        + SUPPORTED_VECTOR_DB_SINK_DATASOURCES.keySet());
                    }
                    return vectorDatabaseSinkConfig.isAgentConfigModelAllowUnknownProperties();
                }
            default:
                throw new IllegalStateException();
        }
    }

    private Class getAgentConfigModelClass(String type, String service) {
        switch (type) {
            case QUERY_VECTOR_DB:
                return QueryVectorDBConfig.class;
            case VECTOR_DB_SINK:
                {
                    final VectorDatabaseWriterConfig vectorDatabaseSinkConfig =
                            SUPPORTED_VECTOR_DB_SINK_DATASOURCES.get(service);
                    if (vectorDatabaseSinkConfig == null) {
                        throw new IllegalArgumentException(
                                "Unsupported vector database service: "
                                        + service
                                        + ". Supported services are: "
                                        + SUPPORTED_VECTOR_DB_SINK_DATASOURCES.keySet());
                    }
                    return vectorDatabaseSinkConfig.getAgentConfigModelClass();
                }
            default:
                throw new IllegalStateException();
        }
    }

    private void generateDataSourceConfiguration(
            String resourceId,
            Application applicationInstance,
            Map<String, Object> configuration,
            ComputeClusterRuntime computeClusterRuntime,
            PluginsRegistry pluginsRegistry,
            AgentConfiguration agentConfiguration) {

        Resource resource = applicationInstance.getResources().get(resourceId);
        log.info("Generating datasource configuration for {}", resourceId);
        if (resource != null) {
            Map<String, Object> resourceConfiguration =
                    computeClusterRuntime.getResourceImplementation(resource, pluginsRegistry);
            if (!resource.type().equals("datasource")
                    && !resource.type().equals("vector-database")) {
                throw new IllegalArgumentException(
                        "Resource '"
                                + resourceId
                                + "' is not type=datasource or type=vector-database");
            }
            configuration.put("datasource", resourceConfiguration);
            final String type = agentConfiguration.getType();
            final String service = (String) resourceConfiguration.get("service");
            final Class modelClass = getAgentConfigModelClass(type, service);
            if (modelClass != null) {
                ClassConfigValidator.validateAgentModelFromClass(
                        agentConfiguration,
                        modelClass,
                        agentConfiguration.getConfiguration(),
                        isAgentConfigModelAllowUnknownProperties(type, service));
            }
        } else {
            throw new IllegalArgumentException("Resource '" + resourceId + "' not found");
        }
    }

    @AgentConfig(
            name = "Query a vector database",
            description =
                    """
                            Query a vector database using Vector Search capabilities.
                            """)
    @Data
    public static class QueryVectorDBConfig extends QueryConfiguration {}

    @Override
    public Map<String, AgentConfigurationModel> generateSupportedTypesDocumentation() {
        Map<String, AgentConfigurationModel> result = new LinkedHashMap<>();
        result.put(
                QUERY_VECTOR_DB,
                ClassConfigValidator.generateAgentModelFromClass(QueryVectorDBConfig.class));

        for (Map.Entry<String, VectorDatabaseWriterConfig> datasource :
                SUPPORTED_VECTOR_DB_SINK_DATASOURCES.entrySet()) {
            final String service = datasource.getKey();
            AgentConfigurationModel value =
                    ClassConfigValidator.generateAgentModelFromClass(
                            datasource.getValue().getAgentConfigModelClass());
            value = deepCopy(value);
            value.getProperties()
                    .get("datasource")
                    .setDescription(
                            "Resource id. The target resource must be type: 'datasource' or 'vector-database' and "
                                    + "service: '"
                                    + service
                                    + "'.");
            value.setType(VECTOR_DB_SINK);
            result.put(VECTOR_DB_SINK + "_" + service, value);
        }
        return result;
    }

    @SneakyThrows
    private static AgentConfigurationModel deepCopy(AgentConfigurationModel instance) {
        return MAPPER.readValue(MAPPER.writeValueAsBytes(instance), AgentConfigurationModel.class);
    }
}
