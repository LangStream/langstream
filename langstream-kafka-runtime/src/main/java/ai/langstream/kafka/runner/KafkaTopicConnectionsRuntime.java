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
package ai.langstream.kafka.runner;

import static ai.langstream.kafka.runtime.KafkaStreamingClusterRuntime.getKafkaClusterRuntimeConfiguration;

import ai.langstream.api.model.Application;
import ai.langstream.api.model.SchemaDefinition;
import ai.langstream.api.model.StreamingCluster;
import ai.langstream.api.model.TopicDefinition;
import ai.langstream.api.runner.topics.TopicAdmin;
import ai.langstream.api.runner.topics.TopicConnectionsRuntime;
import ai.langstream.api.runner.topics.TopicConsumer;
import ai.langstream.api.runner.topics.TopicOffsetPosition;
import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.api.runner.topics.TopicReader;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.Topic;
import ai.langstream.kafka.runtime.KafkaClusterRuntimeConfiguration;
import ai.langstream.kafka.runtime.KafkaTopic;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

@Slf4j
public class KafkaTopicConnectionsRuntime implements TopicConnectionsRuntime {

    @Override
    public TopicReader createReader(
            StreamingCluster streamingCluster,
            Map<String, Object> configuration,
            TopicOffsetPosition initialPosition) {
        Map<String, Object> copy = new HashMap<>(configuration);
        copy.putAll(getKafkaClusterRuntimeConfiguration(streamingCluster).getAdmin());
        copy.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        copy.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // do not use group id for reader. "group.id" default value is null, which is not accepted
        // by KafkaConsumer.
        copy.put("group.id", "");
        // only read one record at the time to have consistent offsets.
        copy.put("max.poll.records", 1);
        String topicName = (String) copy.remove("topic");
        return new KafkaReaderWrapper(copy, topicName, initialPosition);
    }

    @Override
    public TopicConsumer createConsumer(
            String agentId, StreamingCluster streamingCluster, Map<String, Object> configuration) {

        Map<String, Object> copy = new HashMap<>(configuration);
        applyDefaultConfiguration(streamingCluster, copy);
        applyConsumerConfiguration(agentId, copy);
        String topicName = (String) copy.remove("topic");
        copy.remove("deadLetterTopicProducer");

        return new KafkaConsumerWrapper(copy, topicName);
    }

    private void applyConsumerConfiguration(String agentId, Map<String, Object> copy) {
        copy.putIfAbsent(
                "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        copy.putIfAbsent(
                "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        copy.putIfAbsent("enable.auto.commit", "false");
        copy.putIfAbsent("group.id", "langstream-" + agentId);
        copy.putIfAbsent("auto.offset.reset", "earliest");
    }

    private static void applyDefaultConfiguration(
            StreamingCluster streamingCluster, Map<String, Object> copy) {
        KafkaClusterRuntimeConfiguration configuration =
                getKafkaClusterRuntimeConfiguration(streamingCluster);
        copy.putAll(configuration.getAdmin());
    }

    private static void applyProducerConfiguration(Map<String, Object> copy) {
        copy.putIfAbsent(
                "key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        copy.putIfAbsent(
                "value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    }

    @Override
    public TopicProducer createProducer(
            String agentId, StreamingCluster streamingCluster, Map<String, Object> configuration) {
        Map<String, Object> copy = new HashMap<>(configuration);
        applyDefaultConfiguration(streamingCluster, copy);
        applyProducerConfiguration(copy);
        String topicName = (String) copy.remove("topic");

        return new KafkaProducerWrapper(copy, topicName);
    }

    @Override
    public TopicProducer createDeadletterTopicProducer(
            String agentId, StreamingCluster streamingCluster, Map<String, Object> configuration) {
        Map<String, Object> deadletterConfiguration =
                (Map<String, Object>) configuration.get("deadLetterTopicProducer");
        if (deadletterConfiguration == null || deadletterConfiguration.isEmpty()) {
            return null;
        }
        log.info(
                "Creating deadletter topic producer for agent {} using configuration {}",
                agentId,
                configuration);
        return createProducer(agentId, streamingCluster, deadletterConfiguration);
    }

    @Override
    public TopicAdmin createTopicAdmin(
            String agentId, StreamingCluster streamingCluster, Map<String, Object> configuration) {
        Map<String, Object> copy = new HashMap<>(configuration);
        applyDefaultConfiguration(streamingCluster, copy);
        return new TopicAdmin() {

            org.apache.kafka.connect.util.TopicAdmin topicAdmin;

            @Override
            public void start() {
                topicAdmin = new org.apache.kafka.connect.util.TopicAdmin(copy);
            }

            @Override
            public void close() {
                if (topicAdmin != null) {
                    topicAdmin.close();
                    topicAdmin = null;
                }
            }

            @Override
            public Object getNativeTopicAdmin() {
                return topicAdmin;
            }
        };
    }

    @Override
    @SneakyThrows
    public void deploy(ExecutionPlan applicationInstance) {
        Application logicalInstance = applicationInstance.getApplication();
        try (AdminClient admin =
                buildKafkaAdmin(logicalInstance.getInstance().streamingCluster())) {
            for (Topic topic : applicationInstance.getLogicalTopics()) {
                deployTopic(
                        admin,
                        (KafkaTopic) topic,
                        logicalInstance.getInstance().streamingCluster());
            }
        }
    }

    @SneakyThrows
    private void deployTopic(
            AdminClient admin, KafkaTopic topic, StreamingCluster streamingCluster) {
        try {
            switch (topic.createMode()) {
                case TopicDefinition.CREATE_MODE_CREATE_IF_NOT_EXISTS -> {
                    log.info("Creating Kafka topic {}", topic.name());
                    NewTopic newTopic =
                            new NewTopic(
                                    topic.name(),
                                    topic.partitions(),
                                    (short) topic.replicationFactor());
                    if (topic.config() != null) {
                        newTopic.configs(
                                topic.config().entrySet().stream()
                                        .filter(e -> e.getValue() != null)
                                        .collect(
                                                Collectors.toMap(
                                                        Map.Entry::getKey,
                                                        e -> e.getValue().toString())));
                    }
                    admin.createTopics(List.of(newTopic)).all().get();
                    enforceSchemaOnTopic(
                            newTopic, topic.keySchema(), topic.valueSchema(), streamingCluster);
                }
                case TopicDefinition.CREATE_MODE_NONE -> {
                    // do nothing
                }
                default -> throw new IllegalArgumentException(
                        "Unknown create mode " + topic.createMode());
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

    private void enforceSchemaOnTopic(
            NewTopic newTopic,
            SchemaDefinition keySchema,
            SchemaDefinition valueSchema,
            StreamingCluster streamingCluster)
            throws Exception {
        if (keySchema == null && valueSchema == null) {
            return;
        }
        // there is no close() method in this client
        CachedSchemaRegistryClient client = null;

        // here we are using TopicNameStrategy
        // https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#sr-schemas-subject-name-strategy
        SubjectNameStrategy topicNameStrategy = new TopicNameStrategy();
        if (keySchema != null && keySchema.type().equals("avro") && keySchema.schema() != null) {
            ParsedSchema parsedSchema = new AvroSchema(keySchema.schema());
            String subjectName = topicNameStrategy.subjectName(newTopic.name(), true, parsedSchema);
            client = buildSchemaRegistryClient(streamingCluster);
            client.register(subjectName, parsedSchema);
        }

        if (valueSchema != null
                && valueSchema.type().equals("avro")
                && valueSchema.schema() != null) {
            ParsedSchema parsedSchema = new AvroSchema(valueSchema.schema());
            String subjectName =
                    topicNameStrategy.subjectName(newTopic.name(), false, parsedSchema);
            if (client == null) {
                client = buildSchemaRegistryClient(streamingCluster);
            }
            client.register(subjectName, parsedSchema);
        }
    }

    @Override
    @SneakyThrows
    public void delete(ExecutionPlan applicationInstance) {
        Application logicalInstance = applicationInstance.getApplication();
        try (AdminClient admin =
                buildKafkaAdmin(logicalInstance.getInstance().streamingCluster())) {
            for (Topic topic : applicationInstance.getLogicalTopics()) {
                deleteTopic(admin, (KafkaTopic) topic);
            }
        }
    }

    @SneakyThrows
    private void deleteTopic(AdminClient admin, KafkaTopic topic) {
        switch (topic.createMode()) {
            case TopicDefinition.CREATE_MODE_CREATE_IF_NOT_EXISTS -> {
                if (topic.deleteMode().equals(TopicDefinition.DELETE_MODE_DELETE)) {
                    log.info("Deleting Kafka topic {}", topic.name());
                    try {
                        admin.deleteTopics(List.of(topic.name()), new DeleteTopicsOptions())
                                .all()
                                .get();
                    } catch (ExecutionException e) {
                        if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                            log.info("Topic {} does not exist", topic.name());
                        } else {
                            throw e;
                        }
                    }
                } else {
                    log.info(
                            "Keeping Kafka topic {} since deletion-mode is {}",
                            topic.name(),
                            topic.deleteMode());
                }
            }
            default -> log.info(
                    "Keeping Kafka topic {} since creation-mode is {}",
                    topic.name(),
                    topic.createMode());
        }
    }

    private AdminClient buildKafkaAdmin(StreamingCluster streamingCluster) {
        final KafkaClusterRuntimeConfiguration KafkaClusterRuntimeConfiguration =
                getKafkaClusterRuntimeConfiguration(streamingCluster);
        Map<String, Object> adminConfig = KafkaClusterRuntimeConfiguration.getAdmin();
        if (adminConfig == null) {
            adminConfig = new HashMap<>();
        }
        log.info("AdminConfig: {}", adminConfig);
        return KafkaAdminClient.create(adminConfig);
    }

    private CachedSchemaRegistryClient buildSchemaRegistryClient(
            StreamingCluster streamingCluster) {
        final KafkaClusterRuntimeConfiguration KafkaClusterRuntimeConfiguration =
                getKafkaClusterRuntimeConfiguration(streamingCluster);
        Map<String, Object> adminConfig = KafkaClusterRuntimeConfiguration.getAdmin();
        if (adminConfig == null) {
            adminConfig = new HashMap<>();
        }
        log.info("SchemaRegistry client configuration: {}", adminConfig);
        if (!adminConfig.containsKey("schema.registry.url")) {
            throw new IllegalArgumentException(
                    "Missing 'schema.registry.url' property in streaming cluster configuration admin section");
        }
        return new CachedSchemaRegistryClient(
                adminConfig.get("schema.registry.url").toString(), 1000);
    }
}
