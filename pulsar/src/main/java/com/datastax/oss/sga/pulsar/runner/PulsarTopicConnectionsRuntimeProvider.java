package com.datastax.oss.sga.pulsar.runner;

import com.datastax.oss.sga.api.model.StreamingCluster;
import com.datastax.oss.sga.api.runner.code.Header;
import com.datastax.oss.sga.api.runner.code.Record;
import com.datastax.oss.sga.api.runner.topics.TopicAdmin;
import com.datastax.oss.sga.api.runner.topics.TopicConnectionsRuntime;
import com.datastax.oss.sga.api.runner.topics.TopicConnectionsRuntimeProvider;
import com.datastax.oss.sga.api.runner.topics.TopicConsumer;
import com.datastax.oss.sga.api.runner.topics.TopicProducer;
import com.datastax.oss.sga.pulsar.PulsarClientUtils;
import com.datastax.oss.sga.pulsar.PulsarClusterRuntime;
import com.datastax.oss.sga.pulsar.PulsarClusterRuntimeConfiguration;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
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
import java.util.stream.Collectors;

@Slf4j
public class PulsarTopicConnectionsRuntimeProvider implements TopicConnectionsRuntimeProvider {
    @Override
    public boolean supports(String streamingClusterType) {
        return PulsarClusterRuntime.CLUSTER_TYPE.equals(streamingClusterType);
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
                        .loadConf(configuration)
                        .topic(topic)
                        .subscriptionType(SubscriptionType.Failover)
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
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
            Producer<K> producer;
            Schema<K> schema;

            public PulsarTopicProducer(Map<String, Object> configuration) {
                this.configuration = configuration;
            }

            @Override
            @SneakyThrows
            public void start() {
                // TODO: handle schema
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
                                    .collect(Collectors.toMap(Header::key, h -> {
                                        return h.value() != null ? h.value().toString() : null;
                                    })))
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
        }
    }
}
