package com.dastastax.oss.sga.agents;

import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.KafkaOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.util.TopicAdmin;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG;

@Slf4j
public class KafkaSinkTaskContext implements SinkTaskContext, AutoCloseable  {

    // just to get access to baseConfigDef()
    class WorkerConfigImpl extends org.apache.kafka.connect.runtime.WorkerConfig {
        public WorkerConfigImpl(Map<String, String> props) {
            super(baseConfigDef(), props);
        }
    }

    private final Map<String, String> config;

    private final OffsetBackingStore offsetStore;

    // because task.open() needs to be called
    private final java.util.function.Consumer<Collection<TopicPartition>> onPartitionChange;
    private final AtomicBoolean runRepartition = new AtomicBoolean(false);

    private final ConcurrentHashMap<TopicPartition, Long> currentOffsets = new ConcurrentHashMap<>();

    private final Producer<byte[], byte[]> producer;
    private final org.apache.kafka.clients.consumer.Consumer<byte[], byte[]> consumer;
    private final TopicAdmin topicAdmin;
    private final Set<TopicPartition> pausedPartitions = new ConcurrentSkipListSet<>();


    public KafkaSinkTaskContext(Map<String, String> config,
                                Producer<byte[], byte[]> producer,
                                // has to be the same consumer that is used to read data for the task
                                org.apache.kafka.clients.consumer.Consumer<byte[], byte[]> consumer,
                                TopicAdmin topicAdmin,
                                java.util.function.Consumer<Collection<TopicPartition>> onPartitionChange) {
        this.config = config;
        this.topicAdmin = topicAdmin;
        this.consumer = consumer;
        this.producer = producer;

        Converter keyConverter = new JsonConverter();
        keyConverter.configure(Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false"), true);

        Objects.requireNonNull(config.get(OFFSET_STORAGE_TOPIC_CONFIG),
                "offset storage topic must be configured");
        offsetStore = KafkaOffsetBackingStore.forTask(config.get(OFFSET_STORAGE_TOPIC_CONFIG),
                this.producer, this.consumer, this.topicAdmin, keyConverter);

        WorkerConfig workerConfig = new WorkerConfigImpl(config);
        offsetStore.configure(workerConfig);
        offsetStore.start();

        this.onPartitionChange = onPartitionChange;
    }


    @Override
    public void close() {
        try {
            offsetStore.stop();
        } catch (Throwable t) {
            log.error("Failed to stop offset store", t);
        }
    }

    @Override
    public Map<String, String> configs() {
        return config;
    }

    @Override
    public void offset(Map<TopicPartition, Long> map) {
        map.forEach((key, value) -> {
            seekAndUpdateOffset(key, value);
        });

        if (runRepartition.compareAndSet(true, false)) {
            onPartitionChange.accept(currentOffsets.keySet());
        }
    }

    @Override
    public void offset(TopicPartition topicPartition, long l) {
        seekAndUpdateOffset(topicPartition, l);

        if (runRepartition.compareAndSet(true, false)) {
            onPartitionChange.accept(currentOffsets.keySet());
        }
    }

    public Map<TopicPartition, OffsetAndMetadata> currentOffsets() {
        final Map<TopicPartition, OffsetAndMetadata> snapshot = Maps.newHashMapWithExpectedSize(currentOffsets.size());
        currentOffsets.forEach((topicPartition, offset) -> {
            if (offset > 0) {
                snapshot.put(topicPartition,
                        new OffsetAndMetadata(offset, Optional.empty(), null));
            }
        });
        return snapshot;
    }

    private void seekAndUpdateOffset(TopicPartition topicPartition, long offset) {
        try {
            consumer.seek(topicPartition, offset);
        } catch (Throwable e) {
            log.error("Failed to seek topic {} partition {} offset {}",
                    topicPartition.topic(), topicPartition.partition(), offset, e);
            throw new RuntimeException("Failed to seek topic " + topicPartition.topic() + " partition "
                    + topicPartition.partition() + " offset " + offset, e);
        }
        if (!currentOffsets.containsKey(topicPartition)) {
            runRepartition.set(true);
        }
        currentOffsets.put(topicPartition, offset);
    }

    @Override
    public void timeout(long timeoutMs) {
        log.warn("timeout() is called but is not supported currently.");
    }

    @Override
    public Set<TopicPartition> assignment() {
        return currentOffsets.keySet();
    }

    @Override
    public void pause(TopicPartition... partitions) {
        log.debug("Currently paused partitions {}.", partitions);
        pausedPartitions.addAll(Arrays.asList(partitions));
        consumer.pause(Arrays.asList(partitions));
        log.debug("Paused partitions {}.", partitions);
    }

    @Override
    public void resume(TopicPartition... partitions) {
        log.debug("Currently paused partitions {}.", partitions);
        pausedPartitions.removeAll(Arrays.asList(partitions));
        consumer.resume(Arrays.asList(partitions));
        log.debug("{} Resumed partitions: {}", this, partitions);
    }

    @Override
    public void requestCommit() {
        log.warn("requestCommit() is called but is not supported currently.");
    }

    private ByteBuffer topicPartitionAsKey(TopicPartition topicPartition) {
        return ByteBuffer.wrap(topicPartition.toString().getBytes(StandardCharsets.UTF_8));
    }

    private void fillOffsetMap(Map<ByteBuffer, ByteBuffer> offsetMap, TopicPartition topicPartition, long l) {
        ByteBuffer key = topicPartitionAsKey(topicPartition);
        ByteBuffer value = ByteBuffer.allocate(Long.BYTES);
        value.putLong(l);
        value.flip();
        offsetMap.put(key, value);
    }

    public void flushOffsets(Map<TopicPartition, OffsetAndMetadata> offsets) throws Exception {
        Map<ByteBuffer, ByteBuffer> offsetMap = Maps.newHashMapWithExpectedSize(offsets.size());

        offsets.forEach((tp, om) -> fillOffsetMap(offsetMap, tp, om.offset()));
        CompletableFuture<Void> result = new CompletableFuture<>();
        offsetStore.set(offsetMap, (ex, ignore) -> {
            if (ex == null) {
                result.complete(null);
            } else {
                log.error("error flushing offsets for {}", offsets, ex);
                result.completeExceptionally(ex);
            }
        });
        result.get();
    }

}
