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

import ai.langstream.api.runner.code.AbstractAgentCode;
import ai.langstream.api.runner.code.AgentSink;
import ai.langstream.api.runner.code.Record;
import ai.langstream.kafka.runner.KafkaRecord;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.confluent.connect.avro.AvroData;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkConnectorContext;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

/**
 * This is an implementation of {@link AgentSink} that allows you to run any Kafka Connect Sinks. It
 * is a special implementation because it bypasses the LangStream Agent APIs and uses directly the
 * Kafka Consumer. This is needed in order to implement correctly the APIs. It is not expected that
 * this Sink runs together with a custom Source or a Processor, it works only if the Source is
 * directly a Kafka Consumer Source.
 */
@Slf4j
public class KafkaConnectSinkAgent extends AbstractAgentCode implements AgentSink {

    static {
        log.info(
                "Loading KafkaConnectSinkAgent, classloader {}",
                KafkaConnectSinkAgent.class.getClassLoader());
        try {
            Class<?> aClass =
                    KafkaConnectSinkAgent.class
                            .getClassLoader()
                            .loadClass("org.apache.kafka.common.cache.Cache");
            log.info("Loaded class {}", aClass);
        } catch (Throwable t) {
            log.error("Error loading class", t);
        }
    }

    public KafkaConnectSinkAgent() {
        org.apache.kafka.common.cache.Cache<String, String> mockCache = new LRUCache<>(1000);
        mockCache.put("foo", "bar");
        log.info("Created cache {}: {}", mockCache, mockCache.getClass());

        scheduledExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        new ThreadFactoryBuilder()
                                .setNameFormat("kafka-adaptor-sink-flush-%d")
                                .build());
    }

    private AvroData getAvroData() {
        AvroData avroData = lazyAvroData.get();
        if (avroData == null) {
            lazyAvroData.compareAndSet(null, new AvroData(1000));
            avroData = lazyAvroData.get();
        }
        return avroData;
    }

    private static class LangStreamSinkRecord extends SinkRecord {

        final int estimatedSize;

        public LangStreamSinkRecord(
                String topic,
                int partition,
                Schema keySchema,
                Object key,
                Schema valueSchema,
                Object value,
                long kafkaOffset,
                Long timestamp,
                TimestampType timestampType,
                int estimatedSize) {
            super(
                    topic,
                    partition,
                    keySchema,
                    key,
                    valueSchema,
                    value,
                    kafkaOffset,
                    timestamp,
                    timestampType);
            this.estimatedSize = estimatedSize;
        }
    }

    // non null value to avoid NPE in buildAdditionalInfo
    private String kafkaConnectorFQClassName = "?";
    @VisibleForTesting KafkaConnectSinkTaskContext taskContext;
    private SinkConnector connector;
    private SinkTask task;

    private long maxBatchSize;
    private final AtomicLong currentBatchSize = new AtomicLong(0L);

    private long lingerMs;
    private final ScheduledExecutorService scheduledExecutor;
    protected final ConcurrentLinkedDeque<KafkaRecord> pendingFlushQueue =
            new ConcurrentLinkedDeque<>();
    private final AtomicBoolean isFlushRunning = new AtomicBoolean(false);
    private volatile boolean isRunning = false;

    private Map<String, String> kafkaSinkConfig;
    private Map<String, String> adapterConfig;

    private Consumer<?, ?> consumer;

    private final AtomicReference<AvroData> lazyAvroData = new AtomicReference<>();

    private final ConcurrentLinkedDeque<ConsumerCommand> consumerCqrsQueue =
            new ConcurrentLinkedDeque<>();

    private int errorsToInject = 0;

    protected void submitCommand(ConsumerCommand cmd) {
        consumerCqrsQueue.add(cmd);
    }

    @Override
    public boolean handlesCommit() {
        return true;
    }

    @Override
    public CompletableFuture<?> write(Record rec) {
        processed(1, 0);
        if (!isRunning) {
            log.warn("Sink is stopped. Cannot send the records");
            throw new IllegalStateException("Sink is stopped. Cannot send the records");
        }

        SinkRecord sinkRecord;
        KafkaRecord.KafkaConsumerOffsetProvider op;

        try {
            if (errorsToInject > 0) {
                errorsToInject--;
                // otherwise have to muck with schema mismatch and so on
                throw new RuntimeException("Injected record conversion error");
            }
            final KafkaRecord kr = KafkaConnectSinkAgent.getKafkaRecord(rec);
            sinkRecord = toSinkRecord(kr);
            op = (KafkaRecord.KafkaConsumerOffsetProvider) kr;
        } catch (Throwable t) {
            // can throw RuntimeException, let it bubble up
            agentContext
                    .getBadRecordHandler()
                    .handle(
                            rec,
                            t,
                            () -> {
                                log.error("Error handling bad record {}", rec, t);
                                this.close();
                            });
            return CompletableFuture.failedFuture(t);
        }

        try {
            task.put(List.of(sinkRecord));
            currentBatchSize.addAndGet(getRecordSize(op));
            taskContext.updateOffset(op.getTopicPartition(), op.offset());
            pendingFlushQueue.add((KafkaRecord) op);

        } catch (Exception ex) {
            log.error("Error sending the record {}", rec, ex);
            this.close();
            return CompletableFuture.failedFuture(ex);
        }
        flushIfNeeded(false);

        // this is meaningless for this sink, as the sink is handling commits itself
        return CompletableFuture.completedFuture(null);
    }

    private static int getRecordSize(KafkaRecord.KafkaConsumerOffsetProvider r) {
        return r.estimateRecordSize();
    }

    private LangStreamSinkRecord toSinkRecord(KafkaRecord kr) {
        return new LangStreamSinkRecord(
                kr.origin(),
                kr.partition(),
                toKafkaSchema(kr.key(), kr.keySchema()),
                toKafkaData(kr.key(), kr.keySchema()),
                toKafkaSchema(kr.value(), kr.valueSchema()),
                toKafkaData(kr.value(), kr.valueSchema()),
                ((KafkaRecord.KafkaConsumerOffsetProvider) kr).offset(),
                kr.timestamp(),
                kr.timestampType(),
                ((KafkaRecord.KafkaConsumerOffsetProvider) kr).estimateRecordSize());
    }

    private Schema toKafkaSchema(Object input, Schema schema) {
        if (input instanceof GenericData.Record rec && schema == null) {
            return getAvroData().toConnectSchema(rec.getSchema());
        }
        return schema;
    }

    private Object toKafkaData(Object input, Schema schema) {
        if (input instanceof GenericData.Record rec) {
            AvroData avroDataInstance = getAvroData();
            if (schema == null) {
                return avroDataInstance.toConnectData(rec.getSchema(), rec);
            } else {
                return avroDataInstance.toConnectData(
                        avroDataInstance.fromConnectSchema(schema), rec);
            }
        }
        return input;
    }

    private static KafkaRecord getKafkaRecord(Record record) {
        KafkaRecord kr;
        if (record instanceof KafkaRecord) {
            kr = (KafkaRecord) record;
        } else {
            throw new IllegalArgumentException(
                    "Record is not a KafkaRecord but a " + record.getClass());
        }
        return kr;
    }

    private void flushIfNeeded(boolean force) {
        if (isFlushRunning.get()) {
            return;
        }
        if (force || currentBatchSize.get() >= maxBatchSize) {
            scheduledExecutor.submit(this::flush);
        }
    }

    // flush always happens on the same thread
    public void flush() {
        if (log.isDebugEnabled()) {
            log.debug(
                    "flush requested, pending: {}, batchSize: {}",
                    currentBatchSize.get(),
                    maxBatchSize);
        }

        if (pendingFlushQueue.isEmpty()) {
            return;
        }

        if (!isFlushRunning.compareAndSet(false, true)) {
            return;
        }

        final KafkaRecord lastNotFlushed = pendingFlushQueue.getLast();
        Map<TopicPartition, OffsetAndMetadata> committedOffsets;
        try {
            Map<TopicPartition, OffsetAndMetadata> currentOffsets = taskContext.currentOffsets();
            committedOffsets = task.preCommit(currentOffsets);
            if (committedOffsets == null || committedOffsets.isEmpty()) {
                log.info(
                        "Task returned empty committedOffsets map; skipping flush; task will retry later");
                return;
            }
            if (log.isDebugEnabled() && !areMapsEqual(committedOffsets, currentOffsets)) {
                log.debug(
                        "committedOffsets {} differ from currentOffsets {}",
                        committedOffsets,
                        currentOffsets);
            }

            submitCommand(new ConsumerCommand(ConsumerCommand.Command.COMMIT, committedOffsets));
            cleanUpFlushQueueAndUpdateBatchSize(lastNotFlushed, committedOffsets);
            log.info("Flush succeeded");
        } catch (Throwable t) {
            log.error("error flushing pending records", t);
            submitCommand(
                    new ConsumerCommand(
                            ConsumerCommand.Command.THROW,
                            new IllegalStateException("Error flushing pending records", t)));
            this.close();
        } finally {
            isFlushRunning.compareAndSet(true, false);
        }
    }

    // must be called from the same thread as the rest of teh consumer calls
    @Override
    public void commit() throws Exception {

        // can pause create deadlock for the runner?
        while (!consumerCqrsQueue.isEmpty()) {
            ConsumerCommand cmd = consumerCqrsQueue.poll();
            if (cmd == null) {
                break;
            }
            if (log.isDebugEnabled()) {
                log.debug("Executing command {}, ag: {}", cmd.command(), cmd.arg());
            }
            switch (cmd.command()) {
                case COMMIT -> {
                    Map<TopicPartition, OffsetAndMetadata> offsets =
                            (Map<TopicPartition, OffsetAndMetadata>) cmd.arg();
                    consumer.commitSync(offsets);
                }
                case PAUSE -> {
                    List<TopicPartition> partitions = (List<TopicPartition>) cmd.arg();
                    consumer.pause(partitions);
                }
                case RESUME -> {
                    List<TopicPartition> partitions = (List<TopicPartition>) cmd.arg();
                    consumer.resume(partitions);
                }
                case SEEK -> {
                    AbstractMap.SimpleEntry<TopicPartition, Long> arg =
                            (AbstractMap.SimpleEntry<TopicPartition, Long>) cmd.arg();
                    consumer.seek(arg.getKey(), arg.getValue());
                    taskContext.updateOffset(arg.getKey(), arg.getValue());
                }
                case REPARTITION -> {
                    Collection<TopicPartition> partitions = (Collection<TopicPartition>) cmd.arg();
                    task.open(partitions);
                }
                case THROW -> {
                    Exception ex = (Exception) cmd.arg();
                    log.error("Exception throw requested", ex);
                    throw ex;
                }
            }
        }
    }

    @VisibleForTesting
    protected void cleanUpFlushQueueAndUpdateBatchSize(
            KafkaRecord lastNotFlushed, Map<TopicPartition, OffsetAndMetadata> committedOffsets) {
        // lastNotFlushed is needed in case of default preCommit() implementation
        // which calls flush() and returns currentOffsets passed to it.
        // We don't want to ack messages added to pendingFlushQueue after the preCommit/flush call

        for (KafkaRecord r : pendingFlushQueue) {
            OffsetAndMetadata lastCommittedOffset = committedOffsets.get(r.getTopicPartition());

            if (lastCommittedOffset == null) {
                if (r == lastNotFlushed) {
                    break;
                }
                continue;
            }

            if (((KafkaRecord.KafkaConsumerOffsetProvider) r).offset()
                    > lastCommittedOffset.offset()) {
                if (r == lastNotFlushed) {
                    break;
                }
                continue;
            }

            pendingFlushQueue.remove(r);
            currentBatchSize.addAndGet(
                    -1 * getRecordSize((KafkaRecord.KafkaConsumerOffsetProvider) r));
            if (r == lastNotFlushed) {
                break;
            }
        }
    }

    private static boolean areMapsEqual(
            Map<TopicPartition, OffsetAndMetadata> first,
            Map<TopicPartition, OffsetAndMetadata> second) {
        if (first.size() != second.size()) {
            return false;
        }

        return first.entrySet().stream().allMatch(e -> e.getValue().equals(second.get(e.getKey())));
    }

    @Override
    public void init(Map<String, Object> config) {
        if (isRunning) {
            log.warn("Agent already started {} / {}", this.getClass(), kafkaConnectorFQClassName);
            return;
        }
        config = new HashMap<>(config);

        adapterConfig = (Map<String, String>) config.remove("adapterConfig");
        if (adapterConfig == null) {
            adapterConfig = new HashMap<>();
        }

        if (adapterConfig.containsKey("__test_inject_conversion_error")) {
            errorsToInject = Integer.parseInt(adapterConfig.get("__test_inject_conversion_error"));
        }

        kafkaSinkConfig = (Map) config;

        kafkaConnectorFQClassName = (String) config.get("connector.class");
        Objects.requireNonNull(
                kafkaConnectorFQClassName,
                "Kafka connector sink class is not set (connector.class)");

        log.info("Kafka sink started : \n\t{}\n\t{}", kafkaSinkConfig, adapterConfig);
    }

    @SneakyThrows
    @Override
    public void start() {
        if (isRunning) {
            log.warn("Agent already started {} / {}", this.getClass(), kafkaConnectorFQClassName);
            return;
        }
        this.consumer = (Consumer<?, ?>) agentContext.getTopicConsumer().getNativeConsumer();
        log.info("Getting consumer from context {}", consumer);
        Objects.requireNonNull(consumer);
        log.info(
                "Loading class {} from classloader {}",
                kafkaConnectorFQClassName,
                Thread.currentThread().getContextClassLoader());

        Class<?> clazz =
                Class.forName(
                        kafkaConnectorFQClassName,
                        true,
                        Thread.currentThread().getContextClassLoader());
        connector = (SinkConnector) clazz.getConstructor().newInstance();

        Class<? extends Task> taskClass = connector.taskClass();

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
        connector.start(kafkaSinkConfig);

        List<Map<String, String>> configs = connector.taskConfigs(1);
        Preconditions.checkNotNull(configs);
        Preconditions.checkArgument(configs.size() == 1);

        // configs may contain immutable/unmodifiable maps
        configs = configs.stream().map(HashMap::new).collect(Collectors.toList());

        task = (SinkTask) taskClass.getConstructor().newInstance();
        taskContext =
                new KafkaConnectSinkTaskContext(
                        configs.get(0),
                        this::submitCommand,
                        () -> KafkaConnectSinkAgent.this.flushIfNeeded(true));
        task.initialize(taskContext);
        task.start(configs.get(0));

        maxBatchSize = Long.parseLong(adapterConfig.getOrDefault("batchSize", "16384"));
        // kafka's default is 2147483647L but that's too big for normal cases
        lingerMs = Long.parseLong(adapterConfig.getOrDefault("lingerTimeMs", "60000"));

        scheduledExecutor.scheduleWithFixedDelay(
                () -> this.flushIfNeeded(true), lingerMs, lingerMs, TimeUnit.MILLISECONDS);
        isRunning = true;

        log.info("Kafka sink started : \n\t{}\n\t{}", kafkaSinkConfig, adapterConfig);
    }

    @Override
    public void close() {
        if (!isRunning) {
            log.warn("Agent already stopped {} / {}", this.getClass(), kafkaConnectorFQClassName);
            return;
        }

        isRunning = false;
        flushIfNeeded(true);
        scheduledExecutor.shutdown();
        try {
            if (!scheduledExecutor.awaitTermination(10 * lingerMs, TimeUnit.MILLISECONDS)) {
                log.error("scheduledExecutor did not terminate in {} ms", 10 * lingerMs);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("scheduledExecutor's shutdown was interrupted", e);
        }

        try {
            if (task != null) {
                task.stop();
            }
        } catch (Throwable t) {
            log.error("Error stopping the task", t);
        }
        try {
            if (connector != null) {
                connector.stop();
            }
        } catch (Throwable t) {
            log.error("Error stopping the connector", t);
        }

        log.info("Kafka sink stopped.");
    }

    @Override
    protected Map<String, Object> buildAdditionalInfo() {
        return Map.of("connector.class", kafkaConnectorFQClassName);
    }
}
