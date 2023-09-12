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

import com.google.common.collect.Maps;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;

@Slf4j
public class KafkaConnectSinkTaskContext implements SinkTaskContext {

    private final Map<String, String> config;

    private final java.util.function.Consumer<ConsumerCommand> cqrsConsumer;
    private final Runnable onCommitRequest;
    private final AtomicBoolean runRepartition = new AtomicBoolean(false);

    private final ConcurrentHashMap<TopicPartition, Long> currentOffsets =
            new ConcurrentHashMap<>();

    public KafkaConnectSinkTaskContext(
            Map<String, String> config,
            java.util.function.Consumer<ConsumerCommand> cqrsConsumer,
            Runnable onCommitRequest) {
        this.config = config;

        this.cqrsConsumer = cqrsConsumer;
        this.onCommitRequest = onCommitRequest;
    }

    @Override
    public Map<String, String> configs() {
        return config;
    }

    @Override
    public void offset(Map<TopicPartition, Long> map) {
        map.forEach(this::seekAndUpdateOffset);

        if (runRepartition.compareAndSet(true, false)) {
            cqrsConsumer.accept(
                    new ConsumerCommand(
                            ConsumerCommand.Command.REPARTITION, currentOffsets.keySet()));
        }
    }

    @Override
    public void offset(TopicPartition topicPartition, long l) {
        seekAndUpdateOffset(topicPartition, l);

        if (runRepartition.compareAndSet(true, false)) {
            cqrsConsumer.accept(
                    new ConsumerCommand(
                            ConsumerCommand.Command.REPARTITION, currentOffsets.keySet()));
        }
    }

    public Map<TopicPartition, OffsetAndMetadata> currentOffsets() {
        final Map<TopicPartition, OffsetAndMetadata> snapshot =
                Maps.newHashMapWithExpectedSize(currentOffsets.size());
        currentOffsets.forEach(
                (topicPartition, offset) -> {
                    if (offset > 0) {
                        snapshot.put(
                                topicPartition,
                                new OffsetAndMetadata(offset, Optional.empty(), null));
                    }
                });
        return snapshot;
    }

    public void updateOffset(TopicPartition topicPartition, long offset) {
        currentOffsets.put(topicPartition, offset);
    }

    private void seekAndUpdateOffset(TopicPartition topicPartition, long offset) {

        cqrsConsumer.accept(
                new ConsumerCommand(
                        ConsumerCommand.Command.SEEK,
                        new AbstractMap.SimpleEntry<>(topicPartition, offset)));
        if (!currentOffsets.containsKey(topicPartition)) {
            runRepartition.set(true);
        }
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
        cqrsConsumer.accept(
                new ConsumerCommand(ConsumerCommand.Command.PAUSE, Arrays.asList(partitions)));
    }

    @Override
    public void resume(TopicPartition... partitions) {
        log.debug("Resuming partitions: {}", partitions);
        cqrsConsumer.accept(
                new ConsumerCommand(ConsumerCommand.Command.RESUME, Arrays.asList(partitions)));
    }

    @Override
    public void requestCommit() {
        log.info("requestCommit() is called.");
        onCommitRequest.run();
    }
}
