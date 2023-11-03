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
package ai.langstream.pravega;

import static ai.langstream.pravega.PravegaClientUtils.getScope;

import ai.langstream.api.model.Application;
import ai.langstream.api.model.StreamingCluster;
import ai.langstream.api.model.TopicDefinition;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SimpleRecord;
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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PravegaTopicConnectionsRuntimeProvider implements TopicConnectionsRuntimeProvider {
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public boolean supports(String streamingClusterType) {
        return "pravega".equals(streamingClusterType);
    }

    @Override
    public TopicConnectionsRuntime getImplementation() {
        return new PravegaTopicConnectionsRuntime();
    }

    private static class PravegaTopicConnectionsRuntime implements TopicConnectionsRuntime {

        private EventStreamClientFactory client;
        private ReaderGroupManager readerGroupManager;

        private String scope;

        @Override
        @SneakyThrows
        public void init(StreamingCluster streamingCluster) {
            client = PravegaClientUtils.buildPravegaClient(streamingCluster);
            PravegaClusterRuntimeConfiguration pravegaClusterRuntimeConfiguration =
                    PravegaClientUtils.getPravegarClusterRuntimeConfiguration(streamingCluster);
            scope = PravegaClientUtils.getScope(pravegaClusterRuntimeConfiguration);
            readerGroupManager = PravegaClientUtils.buildReaderGroupManager(streamingCluster);
        }

        @Override
        @SneakyThrows
        public void close() {
            if (client != null) {
                client.close();
            }
            if (readerGroupManager != null) {
                readerGroupManager.close();
            }
        }

        @Override
        public TopicReader createReader(
                StreamingCluster streamingCluster,
                Map<String, Object> configuration,
                TopicOffsetPosition initialPosition) {

            String readerGroup = "reader-" + UUID.randomUUID().toString();
            String readerId = "reader-" + UUID.randomUUID().toString();
            String topic = (String) configuration.get("topic");

            // TODO: recover from "initialPosition"

            return new TopicReader() {

                EventStreamReader<String> reader;

                AtomicLong totalOut = new AtomicLong();

                @Override
                public void start() throws Exception {

                    final ReaderGroupConfig readerGroupConfig =
                            ReaderGroupConfig.builder().stream(Stream.of(scope, topic)).build();
                    readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);

                    reader =
                            client.createReader(
                                    readerId,
                                    readerGroup,
                                    new UTF8StringSerializer(),
                                    ReaderConfig.builder().build());
                }

                @Override
                public void close() throws Exception {
                    if (reader != null) {
                        reader.close();
                    }

                    try {
                        readerGroupManager.deleteReaderGroup(readerGroup);
                    } catch (Exception err) {
                        log.info("Error deleting reader group {}", readerGroup, err);
                    }
                }

                @Override
                public TopicReadResult read() throws Exception {
                    EventRead<String> stringEventRead = reader.readNextEvent(1000);
                    log.info("Read event {}", stringEventRead);

                    if (stringEventRead != null
                            && stringEventRead.getEvent() != null
                            && !stringEventRead.isCheckpoint()) {
                        totalOut.incrementAndGet();

                        SimpleRecord build = convertToRecord(stringEventRead, topic);
                        return new TopicReadResult() {
                            @Override
                            public List<Record> records() {
                                return List.of(build);
                            }

                            @Override
                            public byte[] offset() {
                                ByteBuffer position = stringEventRead.getPosition().toBytes();
                                byte[] array = new byte[position.remaining()];
                                position.get(array);
                                return array;
                            }
                        };
                    }
                    return new TopicReadResult() {
                        @Override
                        public List<Record> records() {
                            return List.of();
                        }

                        @Override
                        public byte[] offset() {
                            // TODO
                            return new byte[0];
                        }
                    };
                }
            };
        }

        @Override
        public TopicConsumer createConsumer(
                String agentId,
                StreamingCluster streamingCluster,
                Map<String, Object> configuration) {

            String readerId = agentId;
            String readerGroup = (String) configuration.get("reader-group");
            String topic = (String) configuration.get("topic");
            return new TopicConsumer() {

                EventStreamReader<String> reader;

                AtomicLong totalOut = new AtomicLong();

                @Override
                public Object getNativeConsumer() {
                    return TopicConsumer.super.getNativeConsumer();
                }

                @Override
                public void start() throws Exception {

                    final ReaderGroupConfig readerGroupConfig =
                            ReaderGroupConfig.builder().stream(Stream.of(scope, topic)).build();
                    readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);

                    reader =
                            client.createReader(
                                    readerId,
                                    readerGroup,
                                    new UTF8StringSerializer(),
                                    ReaderConfig.builder().build());
                }

                @Override
                public void close() throws Exception {
                    if (reader != null) {
                        reader.close();
                    }
                }

                @Override
                public List<Record> read() throws Exception {
                    EventRead<String> stringEventRead = reader.readNextEvent(1000);
                    log.info("Read event {}", stringEventRead);

                    if (stringEventRead != null
                            && stringEventRead.getEvent() != null
                            && !stringEventRead.isCheckpoint()) {
                        totalOut.incrementAndGet();

                        SimpleRecord build = convertToRecord(stringEventRead, topic);
                        return List.of(build);
                    }

                    return List.of();
                }

                @Override
                public void commit(List<Record> records) throws Exception {
                    // TODO ?

                }

                @Override
                public Map<String, Object> getInfo() {
                    return Map.of("readerId", readerId, "readerGroup", readerGroup);
                }

                @Override
                public long getTotalOut() {
                    return totalOut.get();
                }
            };
        }

        @Override
        public TopicProducer createProducer(
                String agentId,
                StreamingCluster streamingCluster,
                Map<String, Object> configuration) {
            String topic = (String) configuration.get("topic");

            if (agentId == null) {
                agentId = UUID.randomUUID().toString();
            }

            String producerId = agentId;

            return new TopicProducer() {

                EventStreamWriter<String> eventStreamWriter;

                final AtomicLong totalIn = new AtomicLong();

                @Override
                public void start() {
                    log.info("Creating event stream writer for topic {}", topic);
                    eventStreamWriter =
                            client.createEventWriter(
                                    producerId,
                                    topic,
                                    new UTF8StringSerializer(),
                                    EventWriterConfig.builder().build());
                }

                @Override
                public void close() {
                    if (eventStreamWriter != null) {
                        eventStreamWriter.close();
                    }
                }

                @Override
                public CompletableFuture<?> write(Record record) {
                    log.info("Writing to {} record {}", topic, record);
                    totalIn.incrementAndGet();
                    try {
                        String key = serialiseKey(record.key());
                        String value = serialiseValue(record);
                        log.info("Writing key {} value {}", key, value);
                        if (key != null) {
                            return eventStreamWriter.writeEvent(key, value);
                        } else {
                            return eventStreamWriter.writeEvent(value);
                        }
                    } catch (IOException err) {
                        return CompletableFuture.failedFuture(err);
                    }
                }

                @Override
                public Object getNativeProducer() {
                    return eventStreamWriter;
                }

                @Override
                public Object getInfo() {
                    return Map.of("stream", topic);
                }

                @Override
                public long getTotalIn() {
                    return totalIn.get();
                }
            };
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
            PravegaClusterRuntimeConfiguration configuration =
                    PravegaClientUtils.getPravegarClusterRuntimeConfiguration(
                            logicalInstance.getInstance().streamingCluster());
            try (StreamManager admin = PravegaClientUtils.buildStreamManager(configuration)) {
                String scope = getScope(configuration);
                boolean scopeExists = admin.checkScopeExists(scope);
                if (!scopeExists) {
                    log.info("Creating scope {}", scope);
                    admin.createScope(scope);
                }
                for (Topic topic : applicationInstance.getLogicalTopics()) {
                    deployTopic(admin, (PravegaTopic) topic, scope);
                }
            }
        }

        private static void deployTopic(StreamManager admin, PravegaTopic topic, String scope)
                throws Exception {
            String createMode = topic.createMode();
            StreamConfiguration streamConfig =
                    StreamConfiguration.builder()
                            .scalingPolicy(ScalingPolicy.fixed(topic.partitions()))
                            .build();
            boolean exists = admin.checkStreamExists(scope, topic.name());
            switch (createMode) {
                case TopicDefinition.CREATE_MODE_CREATE_IF_NOT_EXISTS -> {
                    if (!exists) {
                        log.info("Topic {} does not exist, creating", topic.name());
                        admin.createStream(scope, topic.name(), streamConfig);
                    }
                }
                case TopicDefinition.CREATE_MODE_NONE -> {
                    // do nothing
                }
                default -> throw new IllegalArgumentException("Unknown create mode " + createMode);
            }
        }

        private static void deleteTopic(StreamManager admin, PravegaTopic topic, String scope)
                throws Exception {

            switch (topic.createMode()) {
                case TopicDefinition.CREATE_MODE_CREATE_IF_NOT_EXISTS -> {}
                default -> {
                    log.info(
                            "Keeping Pravega stream {} since creation-mode is {}",
                            topic.name(),
                            topic.createMode());
                    return;
                }
            }

            if (!topic.deleteMode().equals(TopicDefinition.DELETE_MODE_DELETE)) {
                log.info(
                        "Keeping Pravega stream {} since deletion-mode is {}",
                        topic.name(),
                        topic.deleteMode());
                return;
            }

            if (!admin.checkStreamExists(scope, topic.name())) {
                return;
            }

            log.info("Deleting topic {} in scope {}", topic.name(), scope);
            try {
                admin.sealStream(scope, topic.name());
                admin.deleteStream(scope, topic.name());
            } catch (Exception error) {
                log.info("Topic {} didn't exit. Not a problem", topic);
            }
        }

        @Override
        @SneakyThrows
        public void delete(ExecutionPlan applicationInstance) {
            Application logicalInstance = applicationInstance.getApplication();
            PravegaClusterRuntimeConfiguration configuration =
                    PravegaClientUtils.getPravegarClusterRuntimeConfiguration(
                            logicalInstance.getInstance().streamingCluster());
            String scope = getScope(configuration);
            try (StreamManager admin =
                    PravegaClientUtils.buildStreamManager(
                            logicalInstance.getInstance().streamingCluster())) {
                for (Topic topic : applicationInstance.getLogicalTopics()) {
                    deleteTopic(admin, (PravegaTopic) topic, scope);
                }
            }
        }
    }

    public static SimpleRecord convertToRecord(EventRead<String> stringEventRead, String topic)
            throws JsonProcessingException {
        Collection<Header> headers = new ArrayList<>();
        log.info("decoding event {}", stringEventRead.getEvent());
        RecordWrapper wrapper = mapper.readValue(stringEventRead.getEvent(), RecordWrapper.class);
        if (wrapper.headers != null) {
            wrapper.headers.forEach(
                    (key, value) -> headers.add(new SimpleRecord.SimpleHeader(key, value)));
        }
        SimpleRecord build =
                SimpleRecord.builder()
                        .key(wrapper.key)
                        .value(wrapper.value)
                        .headers(headers)
                        .timestamp(wrapper.timestamp)
                        .origin(topic)
                        .build();
        return build;
    }

    private static String serialiseKey(Object o) throws IOException {
        if (o == null) {
            return null;
        }

        if (o instanceof CharSequence || o instanceof Number || o.getClass().isPrimitive()) {
            return o.toString();
        }

        return mapper.writeValueAsString(o);
    }

    private static String serialiseValue(Record record) throws IOException {
        Map<String, Object> headers = new HashMap<>();
        if (record.headers() != null) {
            record.headers().forEach(header -> headers.put(header.key(), header.value()));
        }
        RecordWrapper wrapper =
                new RecordWrapper(record.key(), record.value(), headers, record.timestamp());
        return mapper.writeValueAsString(wrapper);
    }

    public record RecordWrapper(
            Object key, Object value, Map<String, Object> headers, Long timestamp) {}
}
