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
package ai.langstream.pulsar;

import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.runtime.AgentNode;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.ExecutionPlanOptimiser;
import com.datastax.oss.sga.api.runtime.StreamingClusterRuntime;
import com.datastax.oss.sga.impl.agents.ai.GenAIToolKitExecutionPlanOptimizer;
import com.datastax.oss.sga.impl.common.BasicClusterRuntime;
import com.datastax.oss.sga.impl.common.DefaultAgentNode;
import ai.langstream.pulsar.agents.PulsarAgentNodeMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.io.SourceConfig;

import java.util.List;
import java.util.Map;

@Slf4j
public class PulsarClusterRuntime extends BasicClusterRuntime {

    static final ObjectMapper mapper = new ObjectMapper();

    public static final String CLUSTER_TYPE = "pulsar";

    static final List<ExecutionPlanOptimiser> OPTIMISERS = List.of(
            new GenAIToolKitExecutionPlanOptimizer());

    @Override
    public String getClusterType() {
        return CLUSTER_TYPE;
    }

    @Override
    public void initialize(Map<String, Object> configuration) {
    }

    @Override
    @SneakyThrows
    public Object deploy(String tenant, ExecutionPlan applicationInstance, StreamingClusterRuntime streamingClusterRuntime,
                         String codeStorageArchiveId) {
        Application logicalInstance = applicationInstance.getApplication();
        streamingClusterRuntime.deploy(applicationInstance);

        try (PulsarAdmin admin = PulsarClientUtils.buildPulsarAdmin(logicalInstance.getInstance().streamingCluster())) {
            for (AgentNode agentImplementation : applicationInstance.getAgents().values()) {
                deployAgent(admin, agentImplementation);
            }
        }

        return null;
    }

    private static void deployAgent(PulsarAdmin admin, AgentNode agent) throws PulsarAdminException {
        if (agent instanceof DefaultAgentNode agentImpl) {
            Object customMetadata = agentImpl.getCustomMetadata();
            if (customMetadata instanceof PulsarAgentNodeMetadata pulsarComponentMetadata) {
                switch (pulsarComponentMetadata.getComponentType()) {
                    case SINK: {
                        deploySink(admin, agentImpl, pulsarComponentMetadata);
                        return;
                    }
                    case SOURCE: {
                        deploySource(admin, agentImpl, pulsarComponentMetadata);
                        return;
                    }
                    case PROCESSOR: {
                        deployFunction(admin, agentImpl, pulsarComponentMetadata);
                        return;
                    }
                    default:
                        throw new IllegalArgumentException("Unsupported Agent type " + agent.getClass().getName());
                }
            }
        }
        throw new IllegalArgumentException("Unsupported Agent type " + agent.getClass().getName());
    }

    private static void deployFunction(PulsarAdmin admin, DefaultAgentNode agentImpl, PulsarAgentNodeMetadata pulsarComponentMetadata) throws PulsarAdminException {
        PulsarName pulsarName = pulsarComponentMetadata.getPulsarName();

        PulsarTopic topicInput = (PulsarTopic) agentImpl.getInputConnectionImplementation();
        String input = topicInput != null ? topicInput.name().toPulsarName() : null;

        PulsarTopic topicOutput = (PulsarTopic) agentImpl.getOutputConnectionImplementation();
        String output = topicOutput != null ? topicOutput.name().toPulsarName() : null;

        String functionType = pulsarComponentMetadata.getAgentType();

        // this is a trick to deploy builtin connectors
        String archiveName = "builtin://" + pulsarComponentMetadata.getAgentType();
        // TODO: plug all the possible configurations
        FunctionConfig functionConfig = FunctionConfig
                .builder()
                .name(pulsarName.name())
                .namespace(pulsarName.namespace())
                .tenant(pulsarName.tenant())
                .inputs(input != null ? List.of(input) : null)
                .output(output)
                .userConfig(agentImpl.getConfiguration())
                .functionType(functionType)
                .jar(archiveName)
                .parallelism(agentImpl
                        .getResourcesSpec().parallelism())
                .build();

        log.info("FunctionConfig: {}", functionConfig);
        admin.functions().createFunction(functionConfig, null);
    }

    private static void deploySource(PulsarAdmin admin, DefaultAgentNode agentImpl, PulsarAgentNodeMetadata pulsarComponentMetadata) throws PulsarAdminException {
        PulsarName pulsarName = pulsarComponentMetadata.getPulsarName();

        PulsarTopic topic = (PulsarTopic) agentImpl.getOutputConnectionImplementation();
        String output = topic.name().toPulsarName();

        // this is a trick to deploy builtin connectors
        String archiveName = "builtin://" + pulsarComponentMetadata.getAgentType();
        // TODO: plug all the possible configurations
        SourceConfig sourceConfig = SourceConfig
                .builder()
                .name(pulsarName.name())
                .namespace(pulsarName.namespace())
                .tenant(pulsarName.tenant())
                .topicName(output)
                .configs(agentImpl.getConfiguration())
                .archive(archiveName)
                .parallelism(1)
                .build();

        log.info("SourceConfiguration: {}", sourceConfig);
        admin.sources().createSource(sourceConfig, null);
    }

    private static void deploySink(PulsarAdmin admin, DefaultAgentNode agentImpl, PulsarAgentNodeMetadata pulsarComponentMetadata) throws PulsarAdminException {
        PulsarName pulsarName = pulsarComponentMetadata.getPulsarName();

        PulsarTopic topic = (PulsarTopic) agentImpl.getInputConnectionImplementation();
        List<String> inputs = List.of(topic.name().toPulsarName());

        // this is a trick to deploy builtin connectors
        String archiveName = "builtin://" + pulsarComponentMetadata.getAgentType();
        // TODO: plug all the possible configurations
        SinkConfig sinkConfig = SinkConfig
                .builder()
                .name(pulsarName.name())
                .namespace(pulsarName.namespace())
                .tenant(pulsarName.tenant())
                .sinkType(pulsarComponentMetadata.getAgentType())
                .configs(agentImpl.getConfiguration())
                .inputs(inputs)
                .archive(archiveName)
                .parallelism(1)
                .retainOrdering(true)
                .build();

        log.info("SinkConfiguration: {}", sinkConfig);
        admin.sinks().createSink(sinkConfig, null);
    }

    @Override
    @SneakyThrows
    public void delete(String tenant, ExecutionPlan applicationInstance, StreamingClusterRuntime streamingClusterRuntime,
                       String codeStorageArchiveId) {
        Application logicalInstance = applicationInstance.getApplication();
        streamingClusterRuntime.delete(applicationInstance);

        try (PulsarAdmin admin = PulsarClientUtils.buildPulsarAdmin(logicalInstance.getInstance().streamingCluster())) {
            for (AgentNode agentImplementation : applicationInstance.getAgents().values()) {
                deleteAgent(admin, agentImplementation);
            }
        }

    }

    private static void deleteAgent(PulsarAdmin admin, AgentNode agent) throws PulsarAdminException {
        if (agent instanceof DefaultAgentNode agentImpl) {
            Object customMetadata = agentImpl.getCustomMetadata();
            if (customMetadata instanceof PulsarAgentNodeMetadata pulsarComponentMetadata) {
                PulsarName pulsarName = pulsarComponentMetadata.getPulsarName();
                try {
                    switch (pulsarComponentMetadata.getComponentType()) {
                        case SINK: {
                            log.info("Deleting Sink {}", pulsarName);
                            admin.sinks().deleteSink(pulsarName.tenant(), pulsarName.namespace(), pulsarName.name());
                            return;
                        }
                        case SOURCE: {
                            log.info("Deleting Source {}", pulsarName);
                            admin.sources().deleteSource(pulsarName.tenant(), pulsarName.namespace(), pulsarName.name());
                            return;
                        }
                        case PROCESSOR: {
                            log.info("Deleting Function {}", pulsarName);
                            admin.functions().deleteFunction(pulsarName.tenant(), pulsarName.namespace(), pulsarName.name());
                            return;
                        }
                        default:
                            throw new IllegalArgumentException("Unsupported Agent type " + agent.getClass().getName());
                    }
                } catch (PulsarAdminException.NotFoundException notFoundException) {
                    log.info("Component {} not found (maybe this it no a problem)", pulsarName, notFoundException);
                }
            }
        }
        throw new IllegalArgumentException("Unsupported Agent type " + agent.getClass().getName());
    }

    @Override
    public List<ExecutionPlanOptimiser> getExecutionPlanOptimisers() {
        return OPTIMISERS;
    }
}
