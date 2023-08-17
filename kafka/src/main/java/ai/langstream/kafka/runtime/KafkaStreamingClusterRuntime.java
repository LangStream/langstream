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
package ai.langstream.kafka.runtime;

import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.SchemaDefinition;
import com.datastax.oss.sga.api.model.StreamingCluster;
import com.datastax.oss.sga.api.model.TopicDefinition;
import com.datastax.oss.sga.api.runtime.AgentNode;
import com.datastax.oss.sga.api.runtime.ConnectionImplementation;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.StreamingClusterRuntime;
import com.datastax.oss.sga.api.runtime.Topic;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.ExecutionException;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

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
    private CachedSchemaRegistryClient buildSchemaRegistryClient(StreamingCluster streamingCluster) throws Exception {
        final KafkaClusterRuntimeConfiguration KafkaClusterRuntimeConfiguration =
                getKafkaClusterRuntimeConfiguration(streamingCluster);
        Map<String, Object> adminConfig = KafkaClusterRuntimeConfiguration.getAdmin();
        if (adminConfig == null) {
            adminConfig = new HashMap<>();
        }
        log.info("SchemaRegistry client configuration: {}", adminConfig);
        return new CachedSchemaRegistryClient(adminConfig.get("schema.registry.url").toString(), 1000);
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
                deployTopic(admin, (KafkaTopic) topic, logicalInstance.getInstance().streamingCluster());
            }

        }
    }

    @SneakyThrows
    private void deployTopic(AdminClient admin, KafkaTopic topic, StreamingCluster streamingCluster) {
        try {
            switch (topic.createMode()) {
                case TopicDefinition.CREATE_MODE_CREATE_IF_NOT_EXISTS: {
                    log.info("Creating Kafka topic {}", topic.name());
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
                    enforceSchemaOnTopic(newTopic, topic.keySchema(), topic.valueSchema(), streamingCluster);
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

    private void enforceSchemaOnTopic(NewTopic newTopic,
                                      SchemaDefinition keySchema, SchemaDefinition valueSchema,
                                      StreamingCluster streamingCluster) throws Exception {
        if (keySchema == null && valueSchema == null) {
            return;
        }
        // there is no close() method in this client
        CachedSchemaRegistryClient client = buildSchemaRegistryClient(streamingCluster);

        // here we are using TopicNameStrategy
        // https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#sr-schemas-subject-name-strategy
        SubjectNameStrategy topicNameStrategy = new TopicNameStrategy();
        if (keySchema != null && keySchema.type().equals("avro") && keySchema.schema() != null) {
            ParsedSchema parsedSchema = new AvroSchema(keySchema.schema());
            String subjectName = topicNameStrategy.subjectName(newTopic.name(), true, parsedSchema);
            client.register(subjectName, parsedSchema);
        }

        if (valueSchema != null && valueSchema.type().equals("avro") && valueSchema.schema() != null) {
            ParsedSchema parsedSchema = new AvroSchema(valueSchema.schema());
            String subjectName = topicNameStrategy.subjectName(newTopic.name(), false, parsedSchema);
            client.register(subjectName, parsedSchema);
        }
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
        switch (topic.createMode()) {
            case TopicDefinition.CREATE_MODE_CREATE_IF_NOT_EXISTS: {
                log.info("Deleting Kafka topic {}", topic.name());
                try {
                    admin.deleteTopics(List.of(topic.name()), new DeleteTopicsOptions()).all().get();
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                        log.info("Topic {} does not exist", topic.name());
                    } else {
                        throw e;
                    }
                }
                break;
            }
            default:
                log.info("Keeping Kafka topic {}", topic.name());
                break;
        }
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
                topicDefinition.isImplicit(),
                configs,
                options);
        return kafkaTopic;
    }

    @Override
    public Map<String, Object> createConsumerConfiguration(AgentNode agentImplementation, ConnectionImplementation inputConnectionImplementation) {
        KafkaTopic kafkaTopic = (KafkaTopic) inputConnectionImplementation;
        Map<String, Object> configuration = new HashMap<>();

        // handle schema
        configuration.putAll(kafkaTopic.createConsumerConfiguration());

        // TODO: handle other configurations
        configuration.computeIfAbsent("group.id", key -> "sga-agent-" + agentImplementation.getId());
        configuration.computeIfAbsent("auto.offset.reset", key -> "earliest");
        return configuration;
    }

    @Override
    public Map<String, Object> createProducerConfiguration(AgentNode agentImplementation, ConnectionImplementation outputConnectionImplementation) {
        KafkaTopic kafkaTopic = (KafkaTopic) outputConnectionImplementation;

        Map<String, Object> configuration = new HashMap<>();
        // handle schema
        configuration.putAll(kafkaTopic.createProducerConfiguration());


        // TODO: handle other configurations

        return configuration;
    }
}
