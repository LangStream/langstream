package com.datastax.oss.sga.pulsar;

import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.StreamingCluster;
import com.datastax.oss.sga.api.runtime.AgentNode;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.StreamingClusterRuntime;
import com.datastax.oss.sga.impl.common.BasicClusterRuntime;
import com.datastax.oss.sga.impl.common.AbstractAgentProvider;
import com.datastax.oss.sga.pulsar.agents.AbstractPulsarFunctionAgentProvider;
import com.datastax.oss.sga.pulsar.agents.AbstractPulsarSinkAgentProvider;
import com.datastax.oss.sga.pulsar.agents.AbstractPulsarSourceAgentProvider;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
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

    @Override
    public String getClusterType() {
        return CLUSTER_TYPE;
    }

    private PulsarAdmin buildPulsarAdmin(StreamingCluster streamingCluster) throws Exception {
        final PulsarClusterRuntimeConfiguration pulsarClusterRuntimeConfiguration =
                getPulsarClusterRuntimeConfiguration(streamingCluster);
        Map<String, Object> adminConfig = pulsarClusterRuntimeConfiguration.getAdmin();
        if (adminConfig == null) {
            adminConfig = new HashMap<>();
        }
        if (adminConfig.get("serviceUrl") == null) {
            adminConfig.put("serviceUrl", "http://localhost:8080");
        }
        return PulsarAdmin
                .builder()
                .loadConf(adminConfig)
                .build();
    }

    public static PulsarClusterRuntimeConfiguration getPulsarClusterRuntimeConfiguration(StreamingCluster streamingCluster) {
        final Map<String, Object> configuration = streamingCluster.configuration();
        return mapper.convertValue(configuration, PulsarClusterRuntimeConfiguration.class);
    }

    @Override
    @SneakyThrows
    public void deploy(ExecutionPlan applicationInstance, StreamingClusterRuntime streamingClusterRuntime) {
        Application logicalInstance = applicationInstance.getApplication();
        streamingClusterRuntime.deploy(applicationInstance);

        try (PulsarAdmin admin = buildPulsarAdmin(logicalInstance.getInstance().streamingCluster())) {
            for (AgentNode agentImplementation : applicationInstance.getAgents().values()) {
                deployAgent(admin, agentImplementation);
            }
        }
    }

    private static void deployAgent(PulsarAdmin admin, AgentNode agent) throws PulsarAdminException {

        if (agent instanceof AbstractAgentProvider.DefaultAgent agentImpl) {
            Object physicalMetadata = agentImpl.getPhysicalMetadata();
            if (physicalMetadata instanceof AbstractPulsarSinkAgentProvider.PulsarSinkMetadata) {
                AbstractPulsarSinkAgentProvider.PulsarSinkMetadata pulsarSinkMetadata =
                        (AbstractPulsarSinkAgentProvider.PulsarSinkMetadata) physicalMetadata;
                PulsarName pulsarName = pulsarSinkMetadata.getPulsarName();

                PulsarTopic topic = (PulsarTopic) agentImpl.getInputConnection();
                List<String> inputs = List.of(topic.name().toPulsarName());

                // this is a trick to deploy builtin connectors
                String archiveName = "builtin://" + pulsarSinkMetadata.getSinkType();
                // TODO: plug all the possible configurations
                SinkConfig sinkConfig = SinkConfig
                        .builder()
                        .name(pulsarName.name())
                        .namespace(pulsarName.namespace())
                        .tenant(pulsarName.tenant())
                        .sinkType(pulsarSinkMetadata.getSinkType())
                        .configs(agentImpl.getConfiguration())
                        .inputs(inputs)
                        .archive(archiveName)
                        .parallelism(1)
                        .retainOrdering(true)
                        .build();

                log.info("SinkConfiguration: {}", sinkConfig);
                admin.sinks().createSink(sinkConfig, null);
                return;
            } else if (physicalMetadata instanceof AbstractPulsarSourceAgentProvider.PulsarSourceMetadata) {
                AbstractPulsarSourceAgentProvider.PulsarSourceMetadata pulsarSource =
                        (AbstractPulsarSourceAgentProvider.PulsarSourceMetadata) physicalMetadata;
                PulsarName pulsarName = pulsarSource.getPulsarName();

                PulsarTopic topic = (PulsarTopic) agentImpl.getOutputConnection();
                String output = topic.name().toPulsarName();

                // this is a trick to deploy builtin connectors
                String archiveName = "builtin://" + pulsarSource.getSourceType();
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
                return;
            } else if (physicalMetadata instanceof AbstractPulsarFunctionAgentProvider.PulsarFunctionMetadata) {
                AbstractPulsarFunctionAgentProvider.PulsarFunctionMetadata pulsarFunction =
                        (AbstractPulsarFunctionAgentProvider.PulsarFunctionMetadata) physicalMetadata;
                PulsarName pulsarName = pulsarFunction.getPulsarName();

                PulsarTopic topicInput = (PulsarTopic) agentImpl.getInputConnection();
                String input = topicInput != null ? topicInput.name().toPulsarName() : null;

                PulsarTopic topicOutput = (PulsarTopic) agentImpl.getOutputConnection();
                String output = topicOutput != null ? topicOutput.name().toPulsarName() : null;

                String functionType = pulsarFunction.getFunctionType();
                String className = pulsarFunction.getFunctionClassname();

                // this is a trick to deploy builtin connectors
                String archiveName = "builtin://" + pulsarFunction.getFunctionType();
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
                        .className(className)
                        .jar(archiveName)
                        .parallelism(1)
                        .build();

                log.info("FunctionConfig: {}", functionConfig);
                admin.functions().createFunction(functionConfig, null);
                return;
            }
        }
        throw new IllegalArgumentException("Unsupported Agent type " + agent.getClass().getName());
    }

    @Override
    public void delete(ExecutionPlan applicationInstance, StreamingClusterRuntime streamingClusterRuntime) {
        throw new UnsupportedOperationException();
    }
}
