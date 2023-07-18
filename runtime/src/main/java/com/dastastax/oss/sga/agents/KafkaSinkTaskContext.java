package com.dastastax.oss.sga.agents;

import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class KafkaSinkTaskContext implements SinkTaskContext  {

    private final Map<String, String> config;

    // because task.open() needs to be called
    private final java.util.function.Consumer<Collection<TopicPartition>> onPartitionChange;
    private final Runnable onCommitRequest;
    private final AtomicBoolean runRepartition = new AtomicBoolean(false);

    private final ConcurrentHashMap<TopicPartition, Long> currentOffsets = new ConcurrentHashMap<>();

    private final org.apache.kafka.clients.consumer.Consumer<?, ?> consumer;
    private final Set<TopicPartition> pausedPartitions = new ConcurrentSkipListSet<>();


    public KafkaSinkTaskContext(Map<String, String> config,
                                // has to be the same consumer that is used to read data for the task
                                org.apache.kafka.clients.consumer.Consumer<?, ?> consumer,
                                java.util.function.Consumer<Collection<TopicPartition>> onPartitionChange,
                                Runnable onCommitRequest) {
        this.config = config;
        this.consumer = consumer;

        this.onPartitionChange = onPartitionChange;
        this.onCommitRequest = onCommitRequest;
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
        log.debug("Pausing partitions {}.", partitions);
        consumer.pause(Arrays.asList(partitions));
        pausedPartitions.addAll(Arrays.asList(partitions));
        log.debug("Currently paused partitions {}.", pausedPartitions);
    }

    @Override
    public void resume(TopicPartition... partitions) {
        log.debug("Resuming partitions: {}", partitions);
        consumer.resume(Arrays.asList(partitions));
        pausedPartitions.removeAll(Arrays.asList(partitions));
        log.debug("Currently paused partitions {}.", pausedPartitions);
    }

    @Override
    public void requestCommit() {
        log.info("requestCommit() is called.");
        onCommitRequest.run();
    }

}
