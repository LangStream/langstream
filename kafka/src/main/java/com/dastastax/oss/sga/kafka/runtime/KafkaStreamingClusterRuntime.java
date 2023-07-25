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
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

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

    public static KafkaClusterRuntimeConfiguration getKafkaClusterRuntimeConfiguration(
            StreamingCluster streamingCluster) {
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
                    admin.createTopics(List.of(new NewTopic(topic.name(), topic.partitions(), (short) 1)))
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
        KafkaTopic kafkaTopic = new KafkaTopic(name,
                topicDefinition.getPartitions() <= 0 ? 1 : topicDefinition.getPartitions(),
                topicDefinition.getKeySchema(),
                topicDefinition.getValueSchema(),
                creationMode);
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

    @Override
    public String receiveMessage(StreamingCluster streamingCluster, TopicDefinition topic, Duration duration) {
        Map<String, Object> configuration = new HashMap<>();
        configuration.put("enable.auto.commit", "true");
        configuration.put("group.id", "sga-" + System.nanoTime());
        configuration.put("auto.offset.reset", "latest");
        configuration.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        configuration.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        final KafkaClusterRuntimeConfiguration kafkaClusterRuntimeConfiguration =
                getKafkaClusterRuntimeConfiguration(streamingCluster);
        final String bootstrapServers = (String) kafkaClusterRuntimeConfiguration.getAdmin()
                .get("bootstrap.servers");
        log.info("Connecting to {}", bootstrapServers);
        configuration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configuration);
        consumer.subscribe(Arrays.asList(topic.getName()));
        ConsumerRecords<String, String> records =
                consumer.poll(duration);

        for (ConsumerRecord<String, String> record : records) {
            return record.value();
        }
        return null;
    }

    @Override
    public void writeMessage(StreamingCluster streamingCluster, TopicDefinition topic, String message) {
        Map<String, Object> configuration = new HashMap<>();
        configuration.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        configuration.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


        final KafkaClusterRuntimeConfiguration kafkaClusterRuntimeConfiguration =
                getKafkaClusterRuntimeConfiguration(streamingCluster);
        final String bootstrapServers = (String) kafkaClusterRuntimeConfiguration.getAdmin()
                .get("bootstrap.servers");
        log.info("Connecting to {}", bootstrapServers);
        configuration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(configuration);
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic.getName(),
                null,
                null, null
                , message.getBytes(
                StandardCharsets.UTF_8),
                null);

        producer.send(record);
    }
}
