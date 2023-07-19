package com.datastax.oss.sga.ai.kafkaconnect;

import com.dastastax.oss.sga.kafka.runner.KafkaTopicConnectionsRuntime;
import com.datastax.oss.sga.api.runner.code.AgentContext;
import com.datastax.oss.sga.api.runner.code.AgentSink;
import com.datastax.oss.sga.api.runner.code.Record;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkConnectorContext;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Slf4j
public class KafkaConnectSinkAgent implements AgentSink {

    private static class SgaSinkRecord extends SinkRecord {

        final int estimatedSize;
        public SgaSinkRecord(String topic, int partition,
                             Schema keySchema, Object key,
                             Schema valueSchema, Object value,
                             long kafkaOffset, Long timestamp,
                             TimestampType timestampType, int estimatedSize) {
            super(topic, partition, keySchema, key, valueSchema, value, kafkaOffset,
                    timestamp, timestampType);
            this.estimatedSize = estimatedSize;
        }
    }

    private String kafkaConnectorFQClassName;
    @VisibleForTesting
    KafkaConnectSinkTaskContext taskContext;
    private SinkConnector connector;
    private SinkTask task;

    private long maxBatchSize;
    private final AtomicLong currentBatchSize = new AtomicLong(0L);

    private long lingerMs;
    private final ScheduledExecutorService scheduledExecutor =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                    .setNameFormat("kafka-adaptor-sink-flush-%d")
                    .build());
    protected final ConcurrentLinkedDeque<KafkaTopicConnectionsRuntime.KafkaRecord> pendingFlushQueue = new ConcurrentLinkedDeque<>();
    //protected final ConcurrentLinkedDeque<KafkaTopicConnectionsRuntime.KafkaRecord> flushedQueue = new ConcurrentLinkedDeque<>();
    private final AtomicBoolean isFlushRunning = new AtomicBoolean(false);
    private volatile boolean isRunning = false;

    private Map<String, String> kafkaSinkConfig;
    private Map<String, String> adapterConfig;

    private Consumer<?, ?> consumer;

    // has to be the same consumer as used to read records to process,
    // otherwise pause/resume won't work
    @Override
    public void setContext(AgentContext context) throws Exception {
        this.consumer = (Consumer<?, ?>)context.getTopicConsumer().getNativeConsumer();
    }


    @Override
    public void write(List<Record> records) {
        if (!isRunning) {
            log.warn("Sink is stopped. Cannot send the records");
            throw new IllegalStateException("Sink is stopped. Cannot send the records");
        }
        try {
            Collection<SinkRecord> sinkRecords = records.stream()
                    .map(this::toSinkRecord)
                    .collect(Collectors.toList());
            task.put(sinkRecords);

            records.stream()
                    .map(x -> {
                        KafkaTopicConnectionsRuntime.KafkaRecord kr = KafkaConnectSinkAgent.getKafkaRecord(x);
                        currentBatchSize.addAndGet(kr.estimateRecordSize());
                        return kr;
                    })
                    .forEach(pendingFlushQueue::add);

        } catch (Exception ex) {
            log.error("Error sending the records {}", records, ex);
            // todo: how to nack? throw for now
            this.close();
            throw new IllegalStateException("Error sending the records", ex);
        }
        flushIfNeeded(false);

//        List<Record> flushedRecords = Lists.newLinkedList();
//        while (!flushedQueue.isEmpty()) {
//            flushedRecords.add(flushedQueue.poll());
//        }
    }

    private static int getRecordSize(KafkaTopicConnectionsRuntime.KafkaRecord r) {
        return r.estimateRecordSize();
    }

    private SgaSinkRecord toSinkRecord(Record record) {
        KafkaTopicConnectionsRuntime.KafkaRecord kr = getKafkaRecord(record);

        return new SgaSinkRecord(kr.origin(),
                kr.partition(),
                kr.keySchema(),
                kr.key(),
                kr.valueSchema(),
                kr.value(),
                kr.offset(),
                kr.timestamp(),
                kr.timestampType(),
                kr.estimateRecordSize());
    }

    private static KafkaTopicConnectionsRuntime.KafkaRecord getKafkaRecord(Record record) {
        KafkaTopicConnectionsRuntime.KafkaRecord kr;
        if (record instanceof KafkaTopicConnectionsRuntime.KafkaRecord) {
            kr = (KafkaTopicConnectionsRuntime.KafkaRecord) record;
        } else {
            throw new IllegalArgumentException("Record is not a KafkaRecord");
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
            log.debug("flush requested, pending: {}, batchSize: {}",
                    currentBatchSize.get(), maxBatchSize);
        }

        if (pendingFlushQueue.isEmpty()) {
            return;
        }

        if (!isFlushRunning.compareAndSet(false, true)) {
            return;
        }

        final KafkaTopicConnectionsRuntime.KafkaRecord lastNotFlushed = pendingFlushQueue.getLast();
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = null;
        try {
            Map<TopicPartition, OffsetAndMetadata> currentOffsets = taskContext.currentOffsets();
            committedOffsets = task.preCommit(currentOffsets);
            if (committedOffsets == null || committedOffsets.isEmpty()) {
                log.info("Task returned empty committedOffsets map; skipping flush; task will retry later");
                return;
            }
            if (log.isDebugEnabled() && !areMapsEqual(committedOffsets, currentOffsets)) {
                log.debug("committedOffsets {} differ from currentOffsets {}", committedOffsets, currentOffsets);
            }
            //taskContext.flushOffsets(committedOffsets);
            ackUntil(lastNotFlushed, committedOffsets, true);
            log.info("Flush succeeded");
        } catch (Throwable t) {
            log.error("error flushing pending records", t);
            ackUntil(lastNotFlushed, committedOffsets, false);
            // todo: how to nack? throw for now
            this.close();
            throw new IllegalStateException("Error flushing pending records", t);
        } finally {
            isFlushRunning.compareAndSet(true, false);
        }
    }

    @VisibleForTesting
    protected void ackUntil(KafkaTopicConnectionsRuntime.KafkaRecord lastNotFlushed,
                            Map<TopicPartition, OffsetAndMetadata> committedOffsets,
                            boolean wasFlushSuccessful) {
        // lastNotFlushed is needed in case of default preCommit() implementation
        // which calls flush() and returns currentOffsets passed to it.
        // We don't want to ack messages added to pendingFlushQueue after the preCommit/flush call

        for (KafkaTopicConnectionsRuntime.KafkaRecord r : pendingFlushQueue) {
            OffsetAndMetadata lastCommittedOffset = committedOffsets.get(r.partition());

            if (lastCommittedOffset == null) {
                if (r == lastNotFlushed) {
                    break;
                }
                continue;
            }

            if (r.offset() > lastCommittedOffset.offset()) {
                if (r == lastNotFlushed) {
                    break;
                }
                continue;
            }

            // todo: how to ack/nack?
            //flushedQueue.add(r);
            pendingFlushQueue.remove(r);
            currentBatchSize.addAndGet(-1 * getRecordSize(r));
            if (r == lastNotFlushed) {
                break;
            }
        }
    }
    private static boolean areMapsEqual(Map<TopicPartition, OffsetAndMetadata> first,
                                        Map<TopicPartition, OffsetAndMetadata> second) {
        if (first.size() != second.size()) {
            return false;
        }

        return first.entrySet().stream()
                .allMatch(e -> e.getValue().equals(second.get(e.getKey())));
    }

    @Override
    public void init(Map<String, Object> config) {
        if (isRunning) {
            log.warn("Agent already started {} / {}", this.getClass(), kafkaConnectorFQClassName);
            return;
        }

        kafkaSinkConfig = (Map<String, String>)config.get("kafkaSinkConfig");
        Objects.requireNonNull(kafkaSinkConfig, "Kafka sink config is not set");

        adapterConfig = (Map<String, String>)config.get("kafkaSinkAdapterConfig");
        Objects.requireNonNull(adapterConfig, "Kafka adapter config is not set");

        kafkaConnectorFQClassName = adapterConfig.get("kafkaConnectorSinkClass");
        Objects.requireNonNull(kafkaConnectorFQClassName, "Kafka connector sink class is not set");

        log.info("Kafka sink started : \n\t{}\n\t{}", kafkaSinkConfig, adapterConfig);
    }

    @SneakyThrows
    @Override
    public void start() {
        if (isRunning) {
            log.warn("Agent already started {} / {}", this.getClass(), kafkaConnectorFQClassName);
            return;
        }

        Class<?> clazz = Class.forName(kafkaConnectorFQClassName);
        connector = (SinkConnector) clazz.getConstructor().newInstance();

        Class<? extends Task> taskClass = connector.taskClass();

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
        connector.start(kafkaSinkConfig);

        List<Map<String, String>> configs = connector.taskConfigs(1);
        Preconditions.checkNotNull(configs);
        Preconditions.checkArgument(configs.size() == 1);

        // configs may contain immutable/unmodifiable maps
        configs = configs.stream()
                .map(HashMap::new)
                .collect(Collectors.toList());

        task = (SinkTask) taskClass.getConstructor().newInstance();
        taskContext = new KafkaConnectSinkTaskContext(configs.get(0),
                consumer,
                task::open,
                () -> KafkaConnectSinkAgent.this.flushIfNeeded(true));
        task.initialize(taskContext);
        task.start(configs.get(0));

        maxBatchSize = Long.parseLong(adapterConfig.getOrDefault("batchSize", "16384"));
        // kafka's default is 2147483647L but that's too big for normal cases
        lingerMs = Long.parseLong(adapterConfig.getOrDefault("lingerTimeMs", "60000"));

        scheduledExecutor.scheduleWithFixedDelay(() ->
                this.flushIfNeeded(true), lingerMs, lingerMs, TimeUnit.MILLISECONDS);
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

}