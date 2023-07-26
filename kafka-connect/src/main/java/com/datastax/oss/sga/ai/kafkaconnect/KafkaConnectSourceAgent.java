package com.datastax.oss.sga.ai.kafkaconnect;

import com.dastastax.oss.sga.kafka.runner.KafkaRecord;
import com.dastastax.oss.sga.kafka.runner.KafkaTopicConnectionsRuntime;
import com.datastax.oss.sga.api.runner.code.AgentContext;
import com.datastax.oss.sga.api.runner.code.AgentSource;
import com.datastax.oss.sga.api.runner.code.Record;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.TaskConfig;
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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG;

@Slf4j
public class KafkaConnectSourceAgent implements AgentSource {

    public static final String ADAPTER_CONFIG = "adapterConfig";
    public static final String CONNECTOR_CLASS = "kafkaConnectorSourceClass";

    private static final String DEFAULT_CONVERTER = "org.apache.kafka.connect.json.JsonConverter";
    private static final String OFFSET_NAMESPACE = "sga-offset-ns";

    private SourceTaskContext sourceTaskContext;
    private SourceConnector connector;
    @Getter
    private SourceTask sourceTask;
    public Converter keyConverter;
    public Converter valueConverter;

    private Producer<byte[], byte[]> producer;
    private Consumer<byte[], byte[]> consumer;
    private TopicAdmin topicAdmin;

    private OffsetBackingStore offsetStore;
    private OffsetStorageReader offsetReader;
    @Getter
    public OffsetStorageWriter offsetWriter;

    private final AtomicInteger outstandingRecords = new AtomicInteger(0);

    Map<String, String> adapterConfig;
    Map<String, String> stringConfig;

    // just to get access to baseConfigDef()
    class WorkerConfigImpl extends org.apache.kafka.connect.runtime.WorkerConfig {
        public WorkerConfigImpl(Map<String, String> props) {
            super(baseConfigDef(), props);
        }
    }

    @Override
    public List<Record> read() throws Exception {

        List<SourceRecord> recordList = sourceTask.poll();
        if (recordList == null || recordList.isEmpty()) {
            return List.of();
        }

        return recordList.stream()
                .map(this::processSourceRecord)
                .toList();
    }

    private Record processSourceRecord(SourceRecord sourceRecord) {
        KafkaRecord kr = KafkaRecord.fromKafkaSourceRecord(sourceRecord);
        offsetWriter.offset(sourceRecord.sourcePartition(), sourceRecord.sourceOffset());
        return kr;
    }

    @Override
    public void commit(List<Record> records) throws Exception {
        log.info("Committing");
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
            completedFlushOffset(flushFuture, t, null);
        }
        flushFuture.get();
    }

    private void completedFlushOffset(CompletableFuture<Void> flushFuture, Throwable error, Void result) {
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
                flushFuture.completeExceptionally(new Exception("Failed to commit offsets", exception));
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

        adapterConfig = (Map<String, String>) configuration.remove(ADAPTER_CONFIG);
        Objects.requireNonNull(adapterConfig, "Adapter configuration is required");
        Objects.requireNonNull(adapterConfig.get(CONNECTOR_CLASS), "Connector class is required");

        stringConfig = Maps.transformValues(configuration, Object::toString);

        // initialize the key and value converter
        keyConverter = Class.forName(stringConfig
                        .getOrDefault(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, DEFAULT_CONVERTER))
                .asSubclass(Converter.class)
                .getDeclaredConstructor()
                .newInstance();
        valueConverter = Class.forName(stringConfig
                        .getOrDefault(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, DEFAULT_CONVERTER))
                .asSubclass(Converter.class)
                .getDeclaredConstructor()
                .newInstance();
        // todo: avro converter, schema registry

        keyConverter.configure(configuration, true);
        valueConverter.configure(configuration, false);

    }

    public void setContext(AgentContext context) throws Exception {
        this.consumer = (Consumer<byte[], byte[]>) context.getTopicConsumer().getNativeConsumer();
        this.producer = (Producer<byte[], byte[]>) context.getTopicProducer().getNativeProducer();
        this.topicAdmin = (TopicAdmin) context.getTopicAdmin();
    }

    @Override
    public void start() throws Exception {
        String kafkaConnectorFQClassName = adapterConfig.get(CONNECTOR_CLASS).toString();
        Class<?> clazz = Class.forName(kafkaConnectorFQClassName);
        connector = (SourceConnector) clazz.getConstructor().newInstance();

        Class<? extends Task> taskClass = connector.taskClass();
        sourceTask = (SourceTask) taskClass.getConstructor().newInstance();

        SinkConnectorContext cnCtx = new SinkConnectorContext() {
            @Override
            public void requestTaskReconfiguration() {
                throw new UnsupportedOperationException("requestTaskReconfiguration is not supported");
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

        Objects.requireNonNull(taskConfig.get(OFFSET_STORAGE_TOPIC_CONFIG),
                "offset storage topic must be configured");
        offsetStore = KafkaOffsetBackingStore.forTask(taskConfig.get(OFFSET_STORAGE_TOPIC_CONFIG),
                this.producer, this.consumer, this.topicAdmin, keyConverter);

        WorkerConfig workerConfig = new WorkerConfigImpl(stringConfig);
        offsetStore.configure(workerConfig);
        offsetStore.start();

        offsetReader = new OffsetStorageReaderImpl(offsetStore,
                OFFSET_NAMESPACE,
                keyConverter,
                valueConverter);

        offsetWriter = new OffsetStorageWriter(offsetStore,
                OFFSET_NAMESPACE,
                keyConverter,
                valueConverter);
        sourceTaskContext = new SourceTaskContext() {
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
            offsetStore.stop();
            offsetStore = null;
        }
    }
}
