package com.datastax.oss.sga.pulsar;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.model.ApplicationInstance;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.Pipeline;
import com.datastax.oss.sga.api.model.StreamingCluster;
import com.datastax.oss.sga.api.model.TopicDefinition;
import com.datastax.oss.sga.api.runtime.AgentImplementation;
import com.datastax.oss.sga.api.runtime.ConnectionImplementation;
import com.datastax.oss.sga.api.runtime.PhysicalApplicationInstance;
import com.datastax.oss.sga.api.runtime.PluginsRegistry;
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

    @Override
    protected void detectAgents(PhysicalApplicationInstance result, StreamingClusterRuntime streamingClusterRuntime, PluginsRegistry pluginsRegistry) {
        ApplicationInstance applicationInstance = result.getApplicationInstance();
        final PulsarClusterRuntimeConfiguration config =
                getPulsarClusterRuntimeConfiguration(applicationInstance.getInstance().streamingCluster());
        String tenant = config.getDefaultTenant();
        String namespace = config.getDefaultNamespace();
        for (Module module : applicationInstance.getModules().values()) {
            if (module.getPipelines() == null) {
                return;
            }
            for (Pipeline pipeline : module.getPipelines().values()) {
                log.info("Pipeline: {}", pipeline.getName());
                for (AgentConfiguration agentConfiguration : pipeline.getAgents().values()) {
                    buildAgent(module, agentConfiguration, result, pluginsRegistry, streamingClusterRuntime);
                }
            }
        }
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
    public void deploy(ApplicationInstance logicalInstance, PhysicalApplicationInstance applicationInstance, StreamingClusterRuntime streamingClusterRuntime) {

        streamingClusterRuntime.deploy(applicationInstance);

        try (PulsarAdmin admin = buildPulsarAdmin(logicalInstance.getInstance().streamingCluster())) {
            for (AgentImplementation agentImplementation : applicationInstance.getAgents().values()) {
                deployAgent(admin, agentImplementation);
            }
        }
    }

    private static void deployAgent(PulsarAdmin admin, AgentImplementation agent) throws PulsarAdminException {

        if (agent instanceof AbstractAgentProvider.DefaultAgentImplementation agentImpl) {
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
    protected ConnectionImplementation buildImplicitTopicForAgent(PhysicalApplicationInstance physicalApplicationInstance, AgentConfiguration agentConfiguration) {
        // connecting two agents requires an intermediate topic
        String name = "agent-" + agentConfiguration.getId() + "-output";
        log.info("Automatically creating topic {} in order to connect agent {}", name,
                agentConfiguration.getId());
        // short circuit...the Pulsar Runtime works only with Pulsar Topics on the same Pulsar Cluster
        String creationMode = TopicDefinition.CREATE_MODE_CREATE_IF_NOT_EXISTS;
        TopicDefinition topicDefinition = new TopicDefinition(name, creationMode, null);
        StreamingCluster streamingCluster = physicalApplicationInstance.getApplicationInstance().getInstance().streamingCluster();
        final PulsarClusterRuntimeConfiguration config =
                getPulsarClusterRuntimeConfiguration(streamingCluster);
        String tenant = config.getDefaultTenant();
        String namespace = config.getDefaultNamespace();
        PulsarName pulsarName = new PulsarName(tenant, namespace, name);
        PulsarTopic pulsarTopic = new PulsarTopic(pulsarName, null, null, null, creationMode);
        physicalApplicationInstance.registerTopic(topicDefinition, pulsarTopic);
        return pulsarTopic;
    }


    @Override
    public void delete(ApplicationInstance logicalInstance, PhysicalApplicationInstance applicationInstance, StreamingClusterRuntime streamingClusterRuntime) {
        throw new UnsupportedOperationException();
    }
}
