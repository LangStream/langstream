package com.datastax.oss.sga.pulsar;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.model.ApplicationInstance;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.Pipeline;
import com.datastax.oss.sga.api.model.SchemaDefinition;
import com.datastax.oss.sga.api.model.StreamingCluster;
import com.datastax.oss.sga.api.model.TopicDefinition;
import com.datastax.oss.sga.api.runtime.AgentImplementation;
import com.datastax.oss.sga.api.runtime.AgentImplementationProvider;
import com.datastax.oss.sga.api.runtime.ClusterRuntime;
import com.datastax.oss.sga.api.runtime.PluginsRegistry;
import com.datastax.oss.sga.impl.common.GenericSinkProvider;
import com.datastax.oss.sga.pulsar.agents.PulsarSinkAgentProvider;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class PulsarClusterRuntime implements ClusterRuntime<PulsarPhysicalApplicationInstance> {

    public static final String CLUSTER_TYPE = "pulsar";

    @Override
    public String getClusterType() {
        return CLUSTER_TYPE;
    }

    @Override
    public PulsarPhysicalApplicationInstance createImplementation(ApplicationInstance applicationInstance, PluginsRegistry pluginsRegistry) {
        StreamingCluster streamingCluster = applicationInstance.getInstance().streamingCluster();

        String tenant = (String) streamingCluster.configuration().getOrDefault("defaultTenant", "public");
        String namespace = (String) streamingCluster.configuration().getOrDefault("defaultNamespace", "default");

        PulsarPhysicalApplicationInstance result = new PulsarPhysicalApplicationInstance(tenant, namespace);

        detectTopics(applicationInstance, result, tenant, namespace);

        detectPipelines(applicationInstance, result, tenant, namespace, pluginsRegistry);


        return result;
    }

    private void detectTopics(ApplicationInstance applicationInstance, PulsarPhysicalApplicationInstance result, String tenant, String namespace) {
        for (Module module : applicationInstance.getModules().values()) {
            for (TopicDefinition topic : module.getTopics().values()) {
                PulsarName topicName
                        = new PulsarName(tenant, namespace, topic.name());
                SchemaDefinition schema = topic.schema();
                String schemaType = schema != null ? schema.type() : null;
                String schemaDefinition = schema != null ? schema.schema() : null;
                String schemaName =  schema != null ? schema.name() : null;
                PulsarTopic pulsarTopic = new PulsarTopic(topicName,
                        schemaName,
                        schemaType,
                        schemaDefinition,
                        topic.creationMode());
                result.getTopics().put(topicName, pulsarTopic);
            }
        }
    }

    private void detectPipelines(ApplicationInstance applicationInstance,
                                 PulsarPhysicalApplicationInstance result,
                                 String tenant, String namespace,
                                 PluginsRegistry pluginsRegistry) {
        for (Module module : applicationInstance.getModules().values()) {
            if (module.getPipelines() == null) {
                return;
            }
            for (Pipeline pipeline : module.getPipelines().values()) {
                log.info("Pipeline: {}", pipeline.getName());
                for (AgentConfiguration agentConfiguration : pipeline.getAgents().values()) {
                    buildAgent(module, agentConfiguration, result, tenant, namespace, pluginsRegistry);
                }
            }
        }
    }

    private void buildAgent(Module module, AgentConfiguration agentConfiguration, PulsarPhysicalApplicationInstance result, String tenant, String namespace ,
                                   PluginsRegistry pluginsRegistry) {
        log.info("Processing agent {} id={} type={}", agentConfiguration.getName(), agentConfiguration.getId(), agentConfiguration.getType());
        AgentImplementationProvider agentImplementationProvider = pluginsRegistry.lookupAgentImplementation(agentConfiguration.getType(), this);

        AgentImplementation agentImplementation = agentImplementationProvider
                .createImplementation(agentConfiguration, module, result, this, pluginsRegistry);

        result.registerAgent(module, agentConfiguration.getId(), agentImplementation);

    }

    private PulsarAdmin buildPulsarAdmin(StreamingCluster streamingCluster) throws Exception {
        return PulsarAdmin
                .builder()
                .serviceHttpUrl((String) streamingCluster.configuration().getOrDefault("webServiceUrl", "http://localhost:8080"))
                .build();
    }

    @Override
    @SneakyThrows
    public void deploy(ApplicationInstance logicalInstance, PulsarPhysicalApplicationInstance applicationInstance) {
        try (PulsarAdmin admin  = buildPulsarAdmin(logicalInstance.getInstance().streamingCluster())) {
            for (PulsarTopic topic : applicationInstance.getTopics().values()) {
                deployTopic(admin, topic);
            }

            for (AgentImplementation agentImplementation : applicationInstance.getAgents().values()) {
                deployAgent(admin, agentImplementation);
            }
        }
    }

    private static void deployTopic(PulsarAdmin admin, PulsarTopic topic) throws PulsarAdminException {
        String createMode = topic.createMode();
        String namespace = topic.name().tenant() + "/" + topic.name().namespace();
        String topicName = topic.name().tenant() + "/" + topic.name().namespace() + "/" + topic.name().name();
        List<String> existing = admin.topics().getList(namespace);
        log.info("Existing topics: {}", existing);
        boolean exists = existing.contains(topicName);
        if (exists) {
            log.info("Topic {} already exists", topicName);
        } else {
            log.info("Topic {} does not exist", topicName);
        }
        switch (createMode) {
            case TopicDefinition.CREATE_MODE_CREATE_IF_NOT_EXISTS: {
                if (!existing.contains(topicName)) {
                    log.info("Topic {} does not exist, creating", topicName);
                    admin
                            .topics().createNonPartitionedTopic(topicName);
                }
                break;
            }
            case TopicDefinition.CREATE_MODE_NONE: {
                // do nothing
                break;
            }
            default:
                throw new IllegalArgumentException("Unknown create mode " + createMode);
        }

        // deploy schema
        if (!StringUtils.isEmpty(topic.schemaType())) {
            List<SchemaInfo> allSchemas = admin.schemas().getAllSchemas(topicName);
            if (allSchemas.isEmpty()) {
                log.info("Deploying schema for topic {}", topicName);
                SchemaInfo schemaInfo = SchemaInfo
                        .builder()
                        .type(SchemaType.valueOf(topic.schemaType().toUpperCase()))
                        .name(topic.schemaName())
                        .properties(Map.of())
                        .schema(topic.schema().getBytes(StandardCharsets.UTF_8))
                        .build();
                admin.schemas().createSchema(topicName, schemaInfo);
            } else {
                log.info("Topic {} already has some schemas, skipping. ({})", topicName, allSchemas);
            }
        }
    }

    private static void deployAgent(PulsarAdmin admin, AgentImplementation agent) throws PulsarAdminException {

        if (agent instanceof GenericSinkProvider.GenericSink sink) {
            PulsarSinkAgentProvider.PulsarSinkMetadata pulsarSinkMetadata = sink.getPhysicalMetadata();
            PulsarName pulsarName = pulsarSinkMetadata.getPulsarName();
            
            PulsarTopic topic = (PulsarTopic) sink.getInputConnection();
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
                    .configs(sink.getConfiguration())
                    .inputs(inputs)
                    .archive(archiveName)
                    .parallelism(1)
                    .retainOrdering(true)
                    .build();

            log.info("SinkConfiguration: {}", sinkConfig);
            admin.sinks().createSink(sinkConfig, null);
            return;
        }
        throw new IllegalArgumentException("Unsupported Agent type " + agent.getClass().getName());
    }



    @Override
    public void delete(ApplicationInstance logicalInstance, PulsarPhysicalApplicationInstance applicationInstance) {
        throw new UnsupportedOperationException();
    }
}
