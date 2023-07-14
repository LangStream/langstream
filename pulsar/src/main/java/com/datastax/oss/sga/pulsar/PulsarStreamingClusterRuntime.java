package com.datastax.oss.sga.pulsar;

import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.SchemaDefinition;
import com.datastax.oss.sga.api.model.StreamingCluster;
import com.datastax.oss.sga.api.model.TopicDefinition;
import com.datastax.oss.sga.api.runtime.AgentNode;
import com.datastax.oss.sga.api.runtime.Connection;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.StreamingClusterRuntime;
import com.datastax.oss.sga.api.runtime.Topic;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaInfo;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class PulsarStreamingClusterRuntime implements StreamingClusterRuntime {

    static final ObjectMapper mapper = new ObjectMapper();


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

    private PulsarClusterRuntimeConfiguration getPulsarClusterRuntimeConfiguration(StreamingCluster streamingCluster) {
        final Map<String, Object> configuration = streamingCluster.configuration();
        return mapper.convertValue(configuration, PulsarClusterRuntimeConfiguration.class);
    }

    @Override
    @SneakyThrows
    public void deploy(ExecutionPlan applicationInstance) {
        Application logicalInstance = applicationInstance.getApplication();
        try (PulsarAdmin admin = buildPulsarAdmin(logicalInstance.getInstance().streamingCluster())) {
            for (Topic topic : applicationInstance.getLogicalTopics()) {
                deployTopic(admin, (PulsarTopic) topic);
            }

        }
    }

    private static void deployTopic(PulsarAdmin admin, PulsarTopic topic) throws PulsarAdminException {
        String createMode = topic.createMode();
        String namespace = topic.name().tenant() + "/" + topic.name().namespace();
        String topicName = topic.name().tenant() + "/" + topic.name().namespace() + "/" + topic.name().name();
        log.info("Listing topics in namespace {}", namespace);
        List<String> existing;
        if (topic.partitions() <= 0) {
            existing = admin.topics().getList(namespace);
        } else {
            existing = admin.topics().getPartitionedTopicList(namespace);
        }
        log.info("Existing topics: {}", existing);
        String fullyQualifiedName = TopicName.get(topicName).toString();
        log.info("Looking for : {}", fullyQualifiedName);
        boolean exists = existing.contains(fullyQualifiedName);
        if (exists) {
            log.info("Topic {} already exists", topicName);
        } else {
            log.info("Topic {} does not exist", topicName);
        }
        switch (createMode) {
            case TopicDefinition.CREATE_MODE_CREATE_IF_NOT_EXISTS: {
                if (!exists) {
                    log.info("Topic {} does not exist, creating", topicName);
                    if (topic.partitions() <= 0) {
                        admin
                                .topics().createNonPartitionedTopic(topicName);
                    } else {
                        admin
                                .topics().createPartitionedTopic(topicName, topic.partitions());
                    }
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
        if (topic.valueSchema() != null) {
            List<SchemaInfo> allSchemas = admin.schemas().getAllSchemas(topicName);
            if (allSchemas.isEmpty()) {
                log.info("Deploying schema for topic {}: {}", topicName, topic.valueSchema());

                SchemaInfo schemaInfo = getSchemaInfo(topic.valueSchema());
                log.info("Value schema {}", schemaInfo);
                if (topic.keySchema() != null) {
                    // KEY VALUE
                    log.info("Deploying key schema for topic {}: {}", topicName, topic.keySchema());
                    SchemaInfo keySchemaInfo = getSchemaInfo(topic.keySchema());
                    log.info("Key schema {}", keySchemaInfo);

                    schemaInfo = KeyValueSchemaInfo
                            .encodeKeyValueSchemaInfo(topic.valueSchema().name(),
                                    keySchemaInfo,
                                    schemaInfo,
                                    KeyValueEncodingType.SEPARATED);

                    log.info("KeyValue schema {}", schemaInfo);

                }

                admin.schemas().createSchema(topicName, schemaInfo);
            } else {
                log.info("Topic {} already has some schemas, skipping. ({})", topicName, allSchemas);
            }
        }
    }

    private static SchemaInfo getSchemaInfo(SchemaDefinition logicalSchemaDefinition) {
        SchemaType pulsarSchemaType = SchemaType.valueOf(logicalSchemaDefinition.type().toUpperCase());
        SchemaInfo keySchemaInfo = SchemaInfo
                .builder()
                .type(pulsarSchemaType)
                .name(logicalSchemaDefinition.name())
                .properties(Map.of())
                .schema(logicalSchemaDefinition.schema() != null ? logicalSchemaDefinition.schema().getBytes(StandardCharsets.UTF_8) : new byte[0])
                .build();
        return keySchemaInfo;
    }

    private static void deleteTopic(PulsarAdmin admin, PulsarTopic topic) throws PulsarAdminException {
        String topicName = topic.name().tenant() + "/" + topic.name().namespace() + "/" + topic.name().name();
        String fullyQualifiedName = TopicName.get(topicName).toString();
        log.info("Deleting topic {}", fullyQualifiedName);
        try {
            if (topic.partitions() <= 0) {
                admin
                        .topics()
                        .delete(fullyQualifiedName, true);
            } else {
                admin
                        .topics()
                        .deletePartitionedTopic(fullyQualifiedName, true);

            }
        } catch (PulsarAdminException.NotFoundException notFoundException) {
            log.info("Topic {} didn't exit. Not a problem", fullyQualifiedName);
        }
    }

    @Override
    @SneakyThrows
    public void delete(ExecutionPlan applicationInstance) {
        Application logicalInstance = applicationInstance.getApplication();
        try (PulsarAdmin admin = buildPulsarAdmin(logicalInstance.getInstance().streamingCluster())) {
            for (Topic topic : applicationInstance.getLogicalTopics()) {
                deleteTopic(admin, (PulsarTopic) topic);
            }
        }
    }

    @Override
    public Topic createTopicImplementation(TopicDefinition topicDefinition, ExecutionPlan applicationInstance) {
        final PulsarClusterRuntimeConfiguration config =
                getPulsarClusterRuntimeConfiguration(applicationInstance.getApplication().getInstance().streamingCluster());

        SchemaDefinition keySchema = topicDefinition.getKeySchema();
        SchemaDefinition valueSchema = topicDefinition.getValueSchema();
        String name = topicDefinition.getName();
        String tenant = config.getDefaultTenant();
        String creationMode = topicDefinition.getCreationMode();
        String namespace = config.getDefaultNamespace();
        PulsarName topicName
                = new PulsarName(tenant, namespace, name);
        PulsarTopic pulsarTopic = new PulsarTopic(topicName,
                topicDefinition.getPartitions(),
                keySchema,
                valueSchema,
                creationMode);
        return pulsarTopic;
    }

    @Override
    public Map<String, Object> createConsumerConfiguration(AgentNode agentImplementation, Connection inputConnection) {
        return Map.of();
    }

    @Override
    public Map<String, Object> createProducerConfiguration(AgentNode agentImplementation, Connection outputConnection) {
        return Map.of();
    }
}
