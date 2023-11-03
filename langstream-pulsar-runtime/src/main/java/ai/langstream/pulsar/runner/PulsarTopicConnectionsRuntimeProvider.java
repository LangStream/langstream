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
package ai.langstream.pulsar.runner;

import static ai.langstream.pulsar.PulsarClientUtils.buildPulsarAdmin;
import static ai.langstream.pulsar.PulsarClientUtils.getPulsarClusterRuntimeConfiguration;
import static java.util.Map.entry;

import ai.langstream.api.model.Application;
import ai.langstream.api.model.SchemaDefinition;
import ai.langstream.api.model.StreamingCluster;
import ai.langstream.api.model.TopicDefinition;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.topics.TopicAdmin;
import ai.langstream.api.runner.topics.TopicConnectionsRuntime;
import ai.langstream.api.runner.topics.TopicConnectionsRuntimeProvider;
import ai.langstream.api.runner.topics.TopicConsumer;
import ai.langstream.api.runner.topics.TopicOffsetPosition;
import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.api.runner.topics.TopicReadResult;
import ai.langstream.api.runner.topics.TopicReader;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.Topic;
import ai.langstream.pulsar.PulsarClientUtils;
import ai.langstream.pulsar.PulsarClusterRuntimeConfiguration;
import ai.langstream.pulsar.PulsarTopic;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaInfo;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

@Slf4j
public class PulsarTopicConnectionsRuntimeProvider implements TopicConnectionsRuntimeProvider {
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public boolean supports(String streamingClusterType) {
        return "pulsar".equals(streamingClusterType);
    }

    @Override
    public TopicConnectionsRuntime getImplementation() {
        return new PulsarTopicConnectionsRuntime();
    }

    private static class PulsarTopicConnectionsRuntime implements TopicConnectionsRuntime {

        private PulsarClient client;

        @Override
        @SneakyThrows
        public void init(StreamingCluster streamingCluster) {
            client = PulsarClientUtils.buildPulsarClient(streamingCluster);
        }

        @Override
        @SneakyThrows
        public void close() {
            if (client != null) {
                client.close();
            }
        }

        @Override
        public TopicReader createReader(
                StreamingCluster streamingCluster,
                Map<String, Object> configuration,
                TopicOffsetPosition initialPosition) {
            Map<String, Object> copy = new HashMap<>(configuration);
            return new PulsarTopicReader(copy, initialPosition);
        }

        @Override
        public TopicConsumer createConsumer(
                String agentId,
                StreamingCluster streamingCluster,
                Map<String, Object> configuration) {
            Map<String, Object> copy = new HashMap<>(configuration);
            copy.remove("deadLetterTopicProducer");
            return new PulsarTopicConsumer(copy);
        }

        @Override
        public TopicProducer createProducer(
                String agentId,
                StreamingCluster streamingCluster,
                Map<String, Object> configuration) {
            Map<String, Object> copy = new HashMap<>(configuration);
            return new PulsarTopicProducer<>(copy);
        }

        @Override
        public TopicProducer createDeadletterTopicProducer(
                String agentId,
                StreamingCluster streamingCluster,
                Map<String, Object> configuration) {
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
                String agentId,
                StreamingCluster streamingCluster,
                Map<String, Object> configuration) {
            return new TopicAdmin() {};
        }

        @Override
        @SneakyThrows
        public void deploy(ExecutionPlan applicationInstance) {
            Application logicalInstance = applicationInstance.getApplication();
            PulsarClusterRuntimeConfiguration configuration =
                    getPulsarClusterRuntimeConfiguration(
                            logicalInstance.getInstance().streamingCluster());
            try (PulsarAdmin admin = buildPulsarAdmin(configuration)) {
                applyRetentionPolicies(
                        admin,
                        "%s/%s"
                                .formatted(
                                        configuration.defaultTenant(),
                                        configuration.defaultNamespace()),
                        configuration.defaultRetentionPolicies());
                for (Topic topic : applicationInstance.getLogicalTopics()) {
                    deployTopic(admin, (PulsarTopic) topic);
                }
            }
        }

        private static void applyRetentionPolicies(
                PulsarAdmin admin,
                String namespace,
                PulsarClusterRuntimeConfiguration.RetentionPolicies policies)
                throws PulsarAdminException {
            if (policies == null) {
                return;
            }
            if (policies.retentionSizeInMB() == null && policies.retentionTimeInMinutes() == null) {
                return;
            }
            admin.namespaces()
                    .setRetention(
                            namespace,
                            new RetentionPolicies(
                                    policies.retentionTimeInMinutes() == null
                                            ? -1
                                            : policies.retentionTimeInMinutes(),
                                    policies.retentionSizeInMB() == null
                                            ? -1
                                            : policies.retentionSizeInMB()));
        }

        private static void deployTopic(PulsarAdmin admin, PulsarTopic topic)
                throws PulsarAdminException {
            String createMode = topic.createMode();
            String namespace = topic.name().tenant() + "/" + topic.name().namespace();
            String topicName =
                    topic.name().tenant()
                            + "/"
                            + topic.name().namespace()
                            + "/"
                            + topic.name().name();
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
                case TopicDefinition.CREATE_MODE_CREATE_IF_NOT_EXISTS -> {
                    if (!exists) {
                        log.info("Topic {} does not exist, creating", topicName);
                        if (topic.partitions() <= 0) {
                            admin.topics().createNonPartitionedTopic(topicName);
                        } else {
                            admin.topics().createPartitionedTopic(topicName, topic.partitions());
                        }
                    }
                }
                case TopicDefinition.CREATE_MODE_NONE -> {
                    // do nothing
                }
                default -> throw new IllegalArgumentException("Unknown create mode " + createMode);
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
                        log.info(
                                "Deploying key schema for topic {}: {}",
                                topicName,
                                topic.keySchema());
                        SchemaInfo keySchemaInfo = getSchemaInfo(topic.keySchema());
                        log.info("Key schema {}", keySchemaInfo);

                        schemaInfo =
                                KeyValueSchemaInfo.encodeKeyValueSchemaInfo(
                                        topic.valueSchema().name(),
                                        keySchemaInfo,
                                        schemaInfo,
                                        KeyValueEncodingType.SEPARATED);

                        log.info("KeyValue schema {}", schemaInfo);
                    }

                    admin.schemas().createSchema(topicName, schemaInfo);
                } else {
                    log.info(
                            "Topic {} already has some schemas, skipping. ({})",
                            topicName,
                            allSchemas);
                }
            }
        }

        private static SchemaInfo getSchemaInfo(SchemaDefinition logicalSchemaDefinition) {
            SchemaType pulsarSchemaType =
                    SchemaType.valueOf(logicalSchemaDefinition.type().toUpperCase());
            return SchemaInfo.builder()
                    .type(pulsarSchemaType)
                    .name(logicalSchemaDefinition.name())
                    .properties(Map.of())
                    .schema(
                            logicalSchemaDefinition.schema() != null
                                    ? logicalSchemaDefinition
                                            .schema()
                                            .getBytes(StandardCharsets.UTF_8)
                                    : new byte[0])
                    .build();
        }

        private static void deleteTopic(PulsarAdmin admin, PulsarTopic topic)
                throws PulsarAdminException {

            switch (topic.createMode()) {
                case TopicDefinition.CREATE_MODE_CREATE_IF_NOT_EXISTS -> {}
                default -> {
                    log.info(
                            "Keeping Pulsar topic {} since creation-mode is {}",
                            topic.name(),
                            topic.createMode());
                    return;
                }
            }

            if (!topic.deleteMode().equals(TopicDefinition.DELETE_MODE_DELETE)) {
                log.info(
                        "Keeping Pulsar topic {} since deletion-mode is {}",
                        topic.name(),
                        topic.deleteMode());
                return;
            }

            String topicName =
                    topic.name().tenant()
                            + "/"
                            + topic.name().namespace()
                            + "/"
                            + topic.name().name();
            String fullyQualifiedName = TopicName.get(topicName).toString();
            log.info("Deleting topic {}", fullyQualifiedName);
            try {
                if (topic.partitions() <= 0) {
                    admin.topics().delete(fullyQualifiedName, true);
                } else {
                    admin.topics().deletePartitionedTopic(fullyQualifiedName, true);
                }
            } catch (PulsarAdminException.NotFoundException notFoundException) {
                log.info("Topic {} didn't exit. Not a problem", fullyQualifiedName);
            }
        }

        @Override
        @SneakyThrows
        public void delete(ExecutionPlan applicationInstance) {
            Application logicalInstance = applicationInstance.getApplication();
            try (PulsarAdmin admin =
                    buildPulsarAdmin(logicalInstance.getInstance().streamingCluster())) {
                for (Topic topic : applicationInstance.getLogicalTopics()) {
                    deleteTopic(admin, (PulsarTopic) topic);
                }
            }
        }

        @ToString
        private static class PulsarConsumerRecord implements Record {
            private final Object finalKey;
            private final Object finalValue;
            private final Message<GenericRecord> receive;

            public PulsarConsumerRecord(
                    Object finalKey, Object finalValue, Message<GenericRecord> receive) {
                this.finalKey = finalKey;
                this.finalValue = finalValue;
                this.receive = receive;
            }

            @Override
            public Object key() {
                return finalKey;
            }

            @Override
            public Object value() {
                return finalValue;
            }

            @Override
            public String origin() {
                return receive.getTopicName();
            }

            @Override
            public Long timestamp() {
                return receive.getPublishTime();
            }

            @Override
            public Collection<Header> headers() {
                return receive.getProperties().entrySet().stream()
                        .map(
                                e ->
                                        new Header() {
                                            @Override
                                            public String key() {
                                                return e.getKey();
                                            }

                                            @Override
                                            public String value() {
                                                return e.getValue();
                                            }

                                            @Override
                                            public String valueAsString() {
                                                return e.getValue();
                                            }
                                        })
                        .collect(Collectors.toList());
            }
        }

        private class PulsarTopicReader implements TopicReader {
            private final Map<String, Object> configuration;
            private final MessageId startMessageId;

            private Map<String, byte[]> topicMessageIds = new HashMap<>();

            private Reader<GenericRecord> reader;

            private PulsarTopicReader(
                    Map<String, Object> configuration, TopicOffsetPosition initialPosition) {
                this.configuration = configuration;
                this.startMessageId =
                        switch (initialPosition.position()) {
                            case Earliest -> MessageId.earliest;
                            case Latest, Absolute -> MessageId.latest;
                        };
                if (initialPosition.position() == TopicOffsetPosition.Position.Absolute) {
                    try {
                        this.topicMessageIds =
                                mapper.readerForMapOf(byte[].class)
                                        .readValue(initialPosition.offset());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }

            @Override
            public void start() throws Exception {
                String topic = (String) configuration.remove("topic");
                reader =
                        client.newReader(Schema.AUTO_CONSUME())
                                .topic(topic)
                                .startMessageId(this.startMessageId)
                                .loadConf(configuration)
                                .create();

                reader.seek(
                        topicPartition -> {
                            try {
                                String topicName = TopicName.get(topicPartition).toString();
                                return MessageId.fromByteArray(
                                        topicMessageIds.getOrDefault(
                                                topicName, startMessageId.toByteArray()));
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
            }

            @Override
            public void close() throws Exception {
                if (reader != null) {
                    reader.close();
                }
            }

            @Override
            public TopicReadResult read() throws Exception {
                Message<GenericRecord> receive = reader.readNext(1, TimeUnit.SECONDS);
                List<Record> records;
                byte[] offset;
                if (receive != null) {
                    Object key = receive.getKey();
                    Object value = receive.getValue().getNativeObject();
                    if (value instanceof KeyValue<?, ?> kv) {
                        key = kv.getKey();
                        value = kv.getValue();
                    }

                    final Object finalKey = key;
                    final Object finalValue = value;
                    log.info("Received message: {}", receive);
                    records = List.of(new PulsarConsumerRecord(finalKey, finalValue, receive));
                    topicMessageIds.put(
                            receive.getTopicName(), receive.getMessageId().toByteArray());
                    offset = mapper.writeValueAsBytes(topicMessageIds);
                } else {
                    records = List.of();
                    offset = null;
                }
                return new TopicReadResult() {
                    @Override
                    public List<Record> records() {
                        return records;
                    }

                    @Override
                    public byte[] offset() {
                        return offset;
                    }
                };
            }
        }

        private class PulsarTopicConsumer implements TopicConsumer {

            private final Map<String, Object> configuration;
            Consumer<GenericRecord> consumer;

            private final AtomicLong totalOut = new AtomicLong();

            public PulsarTopicConsumer(Map<String, Object> configuration) {
                this.configuration = configuration;
            }

            @Override
            public Object getNativeConsumer() {
                return consumer;
            }

            @Override
            public void start() throws Exception {
                String topic = (String) configuration.remove("topic");
                consumer =
                        client.newConsumer(Schema.AUTO_CONSUME())
                                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                                .subscriptionType(SubscriptionType.Failover)
                                .loadConf(configuration)
                                .topic(topic)
                                .subscribe();
            }

            @Override
            public void close() throws Exception {
                if (consumer != null) {
                    consumer.close();
                }
            }

            @Override
            public long getTotalOut() {
                return totalOut.get();
            }

            @Override
            public List<Record> read() throws Exception {
                Message<GenericRecord> receive = consumer.receive(1, TimeUnit.SECONDS);
                if (receive == null) {
                    return List.of();
                }
                Object key = receive.getKey();
                Object value = receive.getValue().getNativeObject();
                if (value instanceof KeyValue<?, ?> kv) {
                    key = kv.getKey();
                    value = kv.getValue();
                }

                final Object finalKey = key;
                final Object finalValue = value;
                log.info("Received message: {}", receive);
                totalOut.incrementAndGet();
                return List.of(new PulsarConsumerRecord(finalKey, finalValue, receive));
            }

            @Override
            public void commit(List<Record> records) throws Exception {
                for (Record record : records) {
                    PulsarConsumerRecord pulsarConsumerRecord = (PulsarConsumerRecord) record;
                    consumer.acknowledge(pulsarConsumerRecord.receive.getMessageId());
                }
            }
        }

        private class PulsarTopicProducer<K> implements TopicProducer {

            private final Map<String, Object> configuration;
            private final AtomicLong totalIn = new AtomicLong();
            String topic;
            Producer<K> producer;
            Schema<K> schema;

            static final Map<Class<?>, Schema<?>> BASE_SCHEMAS =
                    Map.ofEntries(
                            entry(String.class, Schema.STRING),
                            entry(Boolean.class, Schema.BOOL),
                            entry(Byte.class, Schema.INT8),
                            entry(Short.class, Schema.INT16),
                            entry(Integer.class, Schema.INT32),
                            entry(Long.class, Schema.INT64),
                            entry(Float.class, Schema.FLOAT),
                            entry(Double.class, Schema.DOUBLE),
                            entry(byte[].class, Schema.BYTES),
                            entry(Date.class, Schema.DATE),
                            entry(Timestamp.class, Schema.TIMESTAMP),
                            entry(Time.class, Schema.TIME),
                            entry(LocalDate.class, Schema.LOCAL_DATE),
                            entry(LocalTime.class, Schema.LOCAL_TIME),
                            entry(LocalDateTime.class, Schema.LOCAL_DATE_TIME),
                            entry(Instant.class, Schema.INSTANT),
                            entry(ByteBuffer.class, Schema.BYTEBUFFER));

            public PulsarTopicProducer(Map<String, Object> configuration) {
                this.configuration = configuration;
            }

            @Override
            @SneakyThrows
            public void start() {
                topic = (String) configuration.remove("topic");
                if (configuration.containsKey("valueSchema")) {
                    SchemaDefinition valueSchemaDefinition =
                            mapper.convertValue(
                                    configuration.remove("valueSchema"), SchemaDefinition.class);
                    Schema<?> valueSchema = Schema.getSchema(getSchemaInfo(valueSchemaDefinition));
                    if (configuration.containsKey("keySchema")) {
                        SchemaDefinition keySchemaDefinition =
                                mapper.convertValue(
                                        configuration.remove("keySchema"), SchemaDefinition.class);
                        Schema<?> keySchema = Schema.getSchema(getSchemaInfo(keySchemaDefinition));
                        schema =
                                (Schema<K>)
                                        Schema.KeyValue(
                                                keySchema,
                                                valueSchema,
                                                KeyValueEncodingType.SEPARATED);
                    } else {
                        schema = (Schema<K>) valueSchema;
                    }

                    producer =
                            client.newProducer(schema)
                                    .topic(topic)
                                    .loadConf(configuration)
                                    .create();
                }
            }

            @Override
            public Object getNativeProducer() {
                return producer;
            }

            @Override
            @SneakyThrows
            public void close() {
                if (producer != null) {
                    producer.close();
                }
            }

            private Schema<?> getSchema(Class<?> klass) {
                Schema<?> schema = BASE_SCHEMAS.get(klass);
                if (schema == null) {
                    throw new IllegalArgumentException("Cannot infer schema for " + klass);
                }
                return schema;
            }

            @Override
            public CompletableFuture<?> write(Record r) {
                if (topic == null) {
                    throw new RuntimeException("PulsarTopicProducer not started");
                }
                totalIn.addAndGet(1);
                if (schema == null) {
                    try {
                        final Schema<?> valueSchema;
                        if (r.value() != null) {
                            valueSchema = getSchema(r.value().getClass());
                        } else {
                            valueSchema = Schema.BYTES;
                        }
                        if (r.key() != null) {
                            Schema<?> keySchema = getSchema(r.key().getClass());
                            schema =
                                    (Schema<K>)
                                            Schema.KeyValue(
                                                    keySchema,
                                                    valueSchema,
                                                    KeyValueEncodingType.SEPARATED);
                        } else {
                            schema = (Schema<K>) valueSchema;
                        }
                        producer =
                                client.newProducer(schema)
                                        .topic(topic)
                                        .loadConf(configuration)
                                        .create();
                    } catch (Exception e) {
                        return CompletableFuture.failedFuture(e);
                    }
                }

                log.info("Writing message {}", r);

                TypedMessageBuilder<K> message =
                        producer.newMessage()
                                .properties(
                                        r.headers().stream()
                                                .collect(
                                                        Collectors.toMap(
                                                                Header::key,
                                                                h ->
                                                                        h.value() != null
                                                                                ? h.value()
                                                                                        .toString()
                                                                                : null)));

                if (schema instanceof KeyValueSchema<?, ?> keyValueSchema) {
                    KeyValue<?, ?> keyValue =
                            new KeyValue<>(
                                    convertValue(r.key(), keyValueSchema.getKeySchema()),
                                    convertValue(r.value(), keyValueSchema.getValueSchema()));
                    message.value((K) keyValue);
                } else {
                    if (r.key() != null) {
                        message.key(r.key().toString());
                    }
                    message.value((K) convertValue(r.value(), schema));
                }

                return message.sendAsync();
            }

            private Object convertValue(Object value, Schema<?> schema) {
                if (value == null) {
                    return null;
                }
                switch (schema.getSchemaInfo().getType()) {
                    case BYTES:
                        if (value instanceof byte[]) {
                            return value;
                        }
                        return value.toString().getBytes(StandardCharsets.UTF_8);
                    case STRING:
                        return value.toString();
                    default:
                        throw new IllegalArgumentException(
                                "Unsupported output schema type " + schema);
                }
            }

            @Override
            public long getTotalIn() {
                return totalIn.get();
            }
        }
    }
}
