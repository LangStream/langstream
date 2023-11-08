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
package ai.langstream.kafka.runner.kafkaconnect;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.PARTITIONS_VALIDATOR;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.REPLICATION_FACTOR_VALIDATOR;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.CONFIG_TOPIC_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.OFFSET_STORAGE_PARTITIONS_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG;

import ai.langstream.api.runner.code.AbstractAgentCode;
import ai.langstream.api.runner.code.AgentContext;
import ai.langstream.api.runner.code.AgentSource;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.topics.TopicConnectionProvider;
import ai.langstream.api.runner.topics.TopicConsumer;
import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.kafka.runner.KafkaRecord;
import com.google.common.collect.Maps;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.sink.SinkConnectorContext;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.KafkaOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.kafka.connect.util.TopicAdmin;

@Slf4j
public class KafkaConnectSourceAgent extends AbstractAgentCode implements AgentSource {

    public static final String CONNECTOR_CLASS = "connector.class";

    private static final String DEFAULT_CONVERTER = "org.apache.kafka.connect.json.JsonConverter";
    private static final String OFFSET_NAMESPACE = "langstream-offset-ns";

    private SourceTaskContext sourceTaskContext;
    // non null value to avoid NPE in buildAdditionalInfo
    private String kafkaConnectorFQClassName = "?";
    private SourceConnector connector;
    @Getter private SourceTask sourceTask;
    public Converter keyConverter;
    public Converter valueConverter;

    private Producer<byte[], byte[]> producer;
    private Consumer<byte[], byte[]> consumer;
    private TopicConnectionProvider topicConnectionProvider;
    private TopicAdmin topicAdmin;
    private String agentId;

    private OffsetBackingStore offsetStore;
    private boolean offsetStoreStarted;
    private OffsetStorageReader offsetReader;
    @Getter public OffsetStorageWriter offsetWriter;

    Map<String, String> adapterConfig;
    Map<String, String> stringConfig;

    TopicConsumer topicConsumerFromOffsetStore;
    TopicProducer topicProducerToOffsetStore;

    // just to get access to baseConfigDef()
    static class WorkerConfigImpl extends org.apache.kafka.connect.runtime.WorkerConfig {
        public WorkerConfigImpl(Map<String, String> props) {
            super(
                    baseConfigDef()
                            .define(
                                    OFFSET_STORAGE_TOPIC_CONFIG,
                                    ConfigDef.Type.STRING,
                                    ConfigDef.Importance.HIGH,
                                    "")
                            .define(
                                    OFFSET_STORAGE_PARTITIONS_CONFIG,
                                    ConfigDef.Type.INT,
                                    25,
                                    PARTITIONS_VALIDATOR,
                                    ConfigDef.Importance.LOW,
                                    "")
                            .define(
                                    OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG,
                                    ConfigDef.Type.SHORT,
                                    (short) 1,
                                    REPLICATION_FACTOR_VALIDATOR,
                                    ConfigDef.Importance.LOW,
                                    ""),
                    props);
        }
    }

    @Override
    public List<Record> read() throws Exception {

        List<SourceRecord> recordList = sourceTask.poll();
        if (recordList == null || recordList.isEmpty()) {
            return List.of();
        }

        processed(0, recordList.size());

        return recordList.stream().map(this::processSourceRecord).toList();
    }

    private Record processSourceRecord(SourceRecord sourceRecord) {
        KafkaRecord kr = KafkaRecord.fromKafkaSourceRecord(sourceRecord);
        offsetWriter.offset(sourceRecord.sourcePartition(), sourceRecord.sourceOffset());
        return kr;
    }

    @Override
    public void commit(List<Record> records) throws Exception {
        log.info("Committing {}", records);
        try {
            if (!offsetWriter.beginFlush()) {
                log.info("No offsets to commit");
                return;
            }
        } catch (ConnectException ce) {
            log.warn("Flush is already in progress, skipping", ce);
            return;
        }

        final CompletableFuture<Void> flushFuture = new CompletableFuture<>();

        try {
            offsetWriter.doFlush((ex, res) -> completedFlushOffset(flushFuture, ex, res));
        } catch (Throwable t) {
            log.error("Internal error while committing records {}", records, t);
            completedFlushOffset(flushFuture, t, null);
        }
        flushFuture.get();
    }

    private void completedFlushOffset(
            CompletableFuture<Void> flushFuture, Throwable error, Void result) {
        if (error != null) {
            log.error("Failed to flush offsets to storage: ", error);
            offsetWriter.cancelFlush();
            flushFuture.completeExceptionally(new Exception("No Offsets Added Error"));
        } else {
            try {
                sourceTask.commit();

                log.info("Finished flushing offsets to storage");
                flushFuture.complete(null);
            } catch (InterruptedException exception) {
                log.warn("Flush of {} offsets interrupted, cancelling", this);
                Thread.currentThread().interrupt();
                offsetWriter.cancelFlush();
                flushFuture.completeExceptionally(
                        new Exception("Failed to commit offsets", exception));
            } catch (Throwable t) {
                // SourceTask can throw unchecked ConnectException/KafkaException.
                // Make sure the future is cancelled in that case
                log.warn("Flush of {} offsets failed, cancelling", this);
                offsetWriter.cancelFlush();
                flushFuture.completeExceptionally(new Exception("Failed to commit offsets", t));
            }
        }
    }

    @Override
    public void init(Map<String, Object> configuration) throws Exception {
        log.info("Starting Kafka Connect Source Agent with configuration: {}", configuration);

        adapterConfig = (Map) configuration;
        Objects.requireNonNull(adapterConfig.get(CONNECTOR_CLASS), "Connector class is required");

        stringConfig = new HashMap<>(Maps.transformValues(configuration, Object::toString));

        stringConfig.putIfAbsent(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, DEFAULT_CONVERTER);
        stringConfig.putIfAbsent(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, DEFAULT_CONVERTER);
        stringConfig.putIfAbsent(OFFSET_STORAGE_PARTITIONS_CONFIG, "1");
        stringConfig.putIfAbsent(GROUP_ID_CONFIG, agentId);

        stringConfig.putIfAbsent(CONFIG_TOPIC_CONFIG, "youdont'needthis");
        stringConfig.putIfAbsent(STATUS_STORAGE_TOPIC_CONFIG, "youdont'needthis");

        Objects.requireNonNull(
                stringConfig.get(OFFSET_STORAGE_TOPIC_CONFIG),
                "offset storage topic must be configured");

        // initialize the key and value converter
        keyConverter =
                Class.forName(
                                stringConfig.getOrDefault(
                                        WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, DEFAULT_CONVERTER))
                        .asSubclass(Converter.class)
                        .getDeclaredConstructor()
                        .newInstance();
        valueConverter =
                Class.forName(
                                stringConfig.getOrDefault(
                                        WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG,
                                        DEFAULT_CONVERTER))
                        .asSubclass(Converter.class)
                        .getDeclaredConstructor()
                        .newInstance();

        // todo: actual schema registry if configured?
        if (keyConverter instanceof AvroConverter) {
            keyConverter = new AvroConverter(new MockSchemaRegistryClient());
            stringConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock");
        }
        if (valueConverter instanceof AvroConverter) {
            valueConverter = new AvroConverter(new MockSchemaRegistryClient());
            stringConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock");
        }

        keyConverter.configure(configuration, true);
        valueConverter.configure(configuration, false);
    }

    public void setContext(AgentContext context) throws Exception {
        super.setContext(context);
        this.topicAdmin = (TopicAdmin) context.getTopicAdmin().getNativeTopicAdmin();
        this.agentId = context.getGlobalAgentId();
        this.topicConnectionProvider = context.getTopicConnectionProvider();
    }

    @Override
    public void start() throws Exception {
        kafkaConnectorFQClassName = adapterConfig.get(CONNECTOR_CLASS);
        Class<?> clazz = Class.forName(kafkaConnectorFQClassName);
        connector = (SourceConnector) clazz.getConstructor().newInstance();

        Class<? extends Task> taskClass = connector.taskClass();
        sourceTask = (SourceTask) taskClass.getConstructor().newInstance();

        SinkConnectorContext cnCtx =
                new SinkConnectorContext() {
                    @Override
                    public void requestTaskReconfiguration() {
                        throw new UnsupportedOperationException(
                                "requestTaskReconfiguration is not supported");
                    }

                    @Override
                    public void raiseError(Exception e) {
                        throw new UnsupportedOperationException("raiseError is not supported", e);
                    }
                };
        connector.initialize(cnCtx);
        connector.start(stringConfig);

        List<Map<String, String>> configs = connector.taskConfigs(1);
        Objects.requireNonNull(configs);
        checkArgument(configs.size() == 1);
        final Map<String, String> taskConfig = configs.get(0);

        String offsetTopic = stringConfig.get(OFFSET_STORAGE_TOPIC_CONFIG);
        topicConsumerFromOffsetStore =
                topicConnectionProvider.createConsumer(
                        agentId,
                        Map.of(
                                "key.deserializer",
                                "org.apache.kafka.common.serialization.ByteArrayDeserializer",
                                "value.deserializer",
                                "org.apache.kafka.common.serialization.ByteArrayDeserializer"));

        topicConsumerFromOffsetStore.start();
        consumer = (Consumer<byte[], byte[]>) topicConsumerFromOffsetStore.getNativeConsumer();

        topicProducerToOffsetStore =
                topicConnectionProvider.createProducer(
                        agentId,
                        offsetTopic,
                        Map.of(
                                "key.serializer",
                                "org.apache.kafka.common.serialization.ByteArraySerializer",
                                "value.serializer",
                                "org.apache.kafka.common.serialization.ByteArraySerializer"));
        topicProducerToOffsetStore.start();
        producer = (Producer<byte[], byte[]>) topicProducerToOffsetStore.getNativeProducer();

        offsetStore =
                KafkaOffsetBackingStore.forTask(
                        stringConfig.get(OFFSET_STORAGE_TOPIC_CONFIG),
                        this.producer,
                        this.consumer,
                        this.topicAdmin,
                        keyConverter);

        WorkerConfig workerConfig = new WorkerConfigImpl(stringConfig);
        offsetStore.configure(workerConfig);
        offsetStore.start();
        offsetStoreStarted = true;

        offsetReader =
                new OffsetStorageReaderImpl(
                        offsetStore, OFFSET_NAMESPACE, keyConverter, valueConverter);

        offsetWriter =
                new OffsetStorageWriter(
                        offsetStore, OFFSET_NAMESPACE, keyConverter, valueConverter);
        sourceTaskContext =
                new SourceTaskContext() {
                    @Override
                    public Map<String, String> configs() {
                        return taskConfig;
                    }

                    @Override
                    public OffsetStorageReader offsetStorageReader() {
                        return offsetReader;
                    }
                };

        sourceTask.initialize(sourceTaskContext);
        sourceTask.start(taskConfig);
    }

    @Override
    public void close() throws Exception {
        if (sourceTask != null) {
            sourceTask.stop();
            sourceTask = null;
        }

        if (connector != null) {
            connector.stop();
            connector = null;
        }

        if (offsetStore != null) {
            if (offsetStoreStarted) { // Prevent NPE
                offsetStore.stop();
            }
            offsetStore = null;
        }

        if (topicConsumerFromOffsetStore != null) {
            topicConsumerFromOffsetStore.close();
        }

        if (topicProducerToOffsetStore != null) {
            topicProducerToOffsetStore.close();
        }
    }

    @Override
    protected Map<String, Object> buildAdditionalInfo() {
        return Map.of("connector.class", kafkaConnectorFQClassName);
    }
}
