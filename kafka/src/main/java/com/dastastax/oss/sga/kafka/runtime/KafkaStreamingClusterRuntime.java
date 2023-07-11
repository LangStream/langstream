package com.dastastax.oss.sga.kafka.runtime;

import com.datastax.oss.sga.api.model.ApplicationInstance;
import com.datastax.oss.sga.api.model.SchemaDefinition;
import com.datastax.oss.sga.api.model.StreamingCluster;
import com.datastax.oss.sga.api.model.TopicDefinition;
import com.datastax.oss.sga.api.runtime.PhysicalApplicationInstance;
import com.datastax.oss.sga.api.runtime.StreamingClusterRuntime;
import com.datastax.oss.sga.api.runtime.TopicImplementation;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        return KafkaAdminClient.create(adminConfig);
    }

    public static KafkaClusterRuntimeConfiguration getKafkaClusterRuntimeConfiguration(StreamingCluster streamingCluster) {
        final Map<String, Object> configuration = streamingCluster.configuration();
        return mapper.convertValue(configuration, KafkaClusterRuntimeConfiguration.class);
    }

    @Override
    @SneakyThrows
    public void deploy(PhysicalApplicationInstance applicationInstance) {
        ApplicationInstance logicalInstance = applicationInstance.getApplicationInstance();
        try (AdminClient admin = buildKafkaAdmin(logicalInstance.getInstance().streamingCluster())) {
            for (TopicImplementation topic : applicationInstance.getLogicalTopics()) {
                deployTopic(admin, (KafkaTopic) topic);
            }

        }
    }

    private void deployTopic(AdminClient admin, KafkaTopic topic) {
        admin.createTopics(List.of(new NewTopic(topic.name(), 1, (short) 1)));
        // TODO: schema
    }

    @Override
    @SneakyThrows
    public void delete(PhysicalApplicationInstance applicationInstance) {
        ApplicationInstance logicalInstance = applicationInstance.getApplicationInstance();
        try (AdminClient admin = buildKafkaAdmin(logicalInstance.getInstance().streamingCluster())) {
            for (TopicImplementation topic : applicationInstance.getLogicalTopics()) {
                deleteTopic(admin, (KafkaTopic) topic);
            }
        }
    }

    @SneakyThrows
    private void deleteTopic(AdminClient admin, KafkaTopic topic) {
        admin.deleteTopics(List.of(topic.name()), new DeleteTopicsOptions()).all().get();
    }

    @Override
    public TopicImplementation createTopicImplementation(TopicDefinition topicDefinition, PhysicalApplicationInstance applicationInstance) {
        SchemaDefinition schema = topicDefinition.getSchema();
        String name = topicDefinition.getName();
        String creationMode = topicDefinition.getCreationMode();
        String schemaType = schema != null ? schema.type() : null;
        String schemaDefinition = schema != null ? schema.schema() : null;
        String schemaName =  schema != null ? schema.name() : null;
        KafkaTopic pulsarTopic = new KafkaTopic(name,
                schemaName,
                schemaType,
                schemaDefinition,
                creationMode);
        return pulsarTopic;
    }

}
