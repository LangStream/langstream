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

import ai.langstream.api.model.StreamingCluster;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.topics.OffsetPerPartition;
import ai.langstream.api.runner.topics.TopicAdmin;
import ai.langstream.api.runner.topics.TopicConnectionsRuntime;
import ai.langstream.api.runner.topics.TopicConnectionsRuntimeProvider;
import ai.langstream.api.runner.topics.TopicConsumer;
import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.api.runner.topics.TopicReadResult;
import ai.langstream.api.runner.topics.TopicReader;
import ai.langstream.api.runner.topics.TopicOffsetPosition;
import ai.langstream.pulsar.PulsarClientUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.KeyValue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Slf4j
public class PulsarTopicConnectionsRuntimeProvider implements TopicConnectionsRuntimeProvider {
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
        public TopicReader createReader(StreamingCluster streamingCluster,
                                        Map<String, Object> configuration,
                                        TopicOffsetPosition initialPosition) {
            Map<String, Object> copy = new HashMap<>(configuration);
            final TopicConsumer consumer = createConsumer(null, streamingCluster, configuration);
            switch (initialPosition.position()) {
                case Earliest -> copy.put("subscriptionInitialPosition", SubscriptionInitialPosition.Earliest);
                case Latest -> copy.put("subscriptionInitialPosition", SubscriptionInitialPosition.Latest);
                default ->
                    throw new IllegalArgumentException("Unsupported initial position: " + initialPosition.position());
            }
            return new TopicReader() {
                @Override
                public void start() throws Exception {
                    consumer.start();
                }

                @Override
                public void close() throws Exception {
                    consumer.close();
                }

                @Override
                public TopicReadResult read() throws Exception {
                    final List<Record> records = consumer.read();
                    return new TopicReadResult() {
                        @Override
                        public List<Record> records() {
                            return records;
                        }

                        @Override
                        public OffsetPerPartition partitionsOffsets() {
                            return null;
                        }
                    };
                }
            };
        }

        @Override
        public TopicConsumer createConsumer(String agentId, StreamingCluster streamingCluster, Map<String, Object> configuration) {
            Map<String, Object> copy = new HashMap<>(configuration);
            return new PulsarTopicConsumer(copy);
        }

        @Override
        public TopicProducer createProducer(String agentId, StreamingCluster streamingCluster, Map<String, Object> configuration) {
            Map<String, Object> copy = new HashMap<>(configuration);
            return new PulsarTopicProducer(copy);
        }

        @Override
        public TopicAdmin createTopicAdmin(String agentId, StreamingCluster streamingCluster, Map<String, Object> configuration) {
            return new TopicAdmin() {
            };
        }

        private static class PulsarConsumerRecord implements Record {
            private final Object finalKey;
            private final Object finalValue;
            private final Message<GenericRecord> receive;

            public PulsarConsumerRecord(Object finalKey, Object finalValue, Message<GenericRecord> receive) {
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
                        .map(e -> new Header() {
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
                consumer = client
                        .newConsumer(Schema.AUTO_CONSUME())
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                        .loadConf(configuration)
                        .topic(topic)
                        .subscriptionType(SubscriptionType.Failover)
                        .ackTimeout(60000, java.util.concurrent.TimeUnit.MILLISECONDS)
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
                if (value instanceof KeyValue<?,?> kv) {
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

        private class PulsarTopicProducer  <K> implements TopicProducer {

            private final Map<String, Object> configuration;
            private final AtomicLong totalIn = new AtomicLong();
            Producer<K> producer;
            Schema<K> schema;

            public PulsarTopicProducer(Map<String, Object> configuration) {
                this.configuration = configuration;
            }

            @Override
            @SneakyThrows
            public void start() {
                String topic = (String) configuration.remove("topic");
                schema = (Schema<K>) configuration.remove("schema");
                if (schema == null) {
                    schema = (Schema) Schema.BYTES;
                }
                producer = client.newProducer(schema)
                        .topic(topic)
                        .loadConf(configuration)
                        .create();
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

            @Override
            public void write(List<Record> records) {
                totalIn.addAndGet(records.size());
                List<CompletableFuture<?>> handles = new ArrayList<>();
                for (Record r : records) {
                    log.info("Writing message {}", r);
                    // TODO: handle KV
                    handles.add(producer.newMessage()
                            .key(r.key() != null ? r.key().toString() : null)
                            .value(convertValue(r))
                            .properties(r
                                    .headers()
                                    .stream()
                                    .collect(Collectors.toMap(
                                        Header::key,
                                        h -> h.value() != null ? h.value().toString() : null)))
                            .sendAsync());
                }
                CompletableFuture.allOf(handles.toArray(new CompletableFuture[0])).join();
            }

            private K convertValue(Record r) {
                Object value = r.value();
                if (value == null) {
                    return null;
                }
                switch (schema.getSchemaInfo().getType()) {
                    case BYTES:
                        if (value instanceof byte[]) {
                            return (K) value;
                        }
                        return (K) value.toString().getBytes(StandardCharsets.UTF_8);
                    case STRING:
                        return (K) value.toString();
                    default:
                        throw new IllegalArgumentException("Unsupported output schema type " + schema);
                }
            }

            @Override
            public long getTotalIn() {
                return totalIn.get();
            }
        }
    }
}
