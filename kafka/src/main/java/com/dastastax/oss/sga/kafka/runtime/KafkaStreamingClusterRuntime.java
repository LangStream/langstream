package com.dastastax.oss.sga.kafka.runtime;

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
import java.util.concurrent.ExecutionException;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class KafkaStreamingClusterRuntime implements StreamingClusterRuntime {

    static final ObjectMapper mapper = new ObjectMapper();


    private AdminClient buildKafkaAdmin(StreamingCluster streamingCluster) throws Exception {
        final KafkaClusterRuntimeConfiguration KafkaClusterRuntimeConfiguration =
                getKafkaClusterRuntimeConfiguration(streamingCluster);
        Map<String, Object> adminConfig = KafkaClusterRuntimeConfiguration.getAdmin();
        if (adminConfig == null) {
            adminConfig = new HashMap<>();
        }
        log.info("AdminConfig: {}", adminConfig);
        return KafkaAdminClient.create(adminConfig);
    }

    public static KafkaClusterRuntimeConfiguration getKafkaClusterRuntimeConfiguration(StreamingCluster streamingCluster) {
        final Map<String, Object> configuration = streamingCluster.configuration();
        return mapper.convertValue(configuration, KafkaClusterRuntimeConfiguration.class);
    }

    @Override
    @SneakyThrows
    public void deploy(ExecutionPlan applicationInstance) {
        Application logicalInstance = applicationInstance.getApplication();
        try (AdminClient admin = buildKafkaAdmin(logicalInstance.getInstance().streamingCluster())) {
            for (Topic topic : applicationInstance.getLogicalTopics()) {
                deployTopic(admin, (KafkaTopic) topic);
            }

        }
    }

    @SneakyThrows
    private void deployTopic(AdminClient admin, KafkaTopic topic) {
        try {
            switch (topic.createMode()) {
                case TopicDefinition.CREATE_MODE_CREATE_IF_NOT_EXISTS: {
                    log.info("Creating topic {}", topic.name());
                    NewTopic newTopic = new NewTopic(topic.name(), topic.partitions(), (short) 1);
                    if (topic.config() != null) {
                        newTopic.configs(topic
                                .config()
                                .entrySet()
                                .stream()
                                .filter(e -> e.getValue() != null)
                                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString())));
                    }
                    admin.createTopics(List.of(newTopic))
                            .all()
                            .get();
                    break;
                }
                case TopicDefinition.CREATE_MODE_NONE: {
                    // do nothing
                    break;
                }
                default:
                    throw new IllegalArgumentException("Unknown create mode " + topic.createMode());
            }
        } catch (ExecutionException e) {
            if (e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException) {
                log.info("Topic {} already exists", topic.name());
            } else {
                throw e;
            }
        }
        // TODO: schema
    }

    @Override
    @SneakyThrows
    public void delete(ExecutionPlan applicationInstance) {
        Application logicalInstance = applicationInstance.getApplication();
        try (AdminClient admin = buildKafkaAdmin(logicalInstance.getInstance().streamingCluster())) {
            for (Topic topic : applicationInstance.getLogicalTopics()) {
                deleteTopic(admin, (KafkaTopic) topic);
            }
        }
    }

    @SneakyThrows
    private void deleteTopic(AdminClient admin, KafkaTopic topic) {
        admin.deleteTopics(List.of(topic.name()), new DeleteTopicsOptions()).all().get();
    }

    @Override
    public Topic createTopicImplementation(TopicDefinition topicDefinition, ExecutionPlan applicationInstance) {
        String name = topicDefinition.getName();
        String creationMode = topicDefinition.getCreationMode();
        Map<String, Object> options = topicDefinition.getOptions();
        if (options == null) {
            options = new HashMap<>();
        }
        int replicationFactor = Integer.parseInt(options.getOrDefault("replication-factor", "1").toString());
        Map<String, Object> configs = topicDefinition.getConfig();
        KafkaTopic kafkaTopic = new KafkaTopic(name,
                topicDefinition.getPartitions() <= 0 ? 1 : topicDefinition.getPartitions(),
                replicationFactor,
                topicDefinition.getKeySchema(),
                topicDefinition.getValueSchema(),
                creationMode,
                configs);
        return kafkaTopic;
    }

    @Override
    public Map<String, Object> createConsumerConfiguration(AgentNode agentImplementation, Connection inputConnection) {
        KafkaTopic kafkaTopic = (KafkaTopic) inputConnection;
        Map<String, Object> configuration = new HashMap<>();

        // handle schema
        configuration.putAll(kafkaTopic.createConsumerConfiguration());

        // TODO: handle other configurations
        configuration.computeIfAbsent("group.id", key -> "sga-agent-" + agentImplementation.getId());
        configuration.computeIfAbsent("auto.offset.reset", key -> "earliest");
        return configuration;
    }

    @Override
    public Map<String, Object> createProducerConfiguration(AgentNode agentImplementation, Connection outputConnection) {
        KafkaTopic kafkaTopic = (KafkaTopic) outputConnection;

        Map<String, Object> configuration = new HashMap<>();
        // handle schema
        configuration.putAll(kafkaTopic.createProducerConfiguration());


        // TODO: handle other configurations

        return configuration;
    }
}
