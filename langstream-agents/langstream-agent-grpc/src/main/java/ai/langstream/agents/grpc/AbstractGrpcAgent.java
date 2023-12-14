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
package ai.langstream.agents.grpc;

import ai.langstream.api.runner.code.AbstractAgentCode;
import ai.langstream.api.runner.code.SimpleRecord;
import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.api.util.ConfigurationUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Conversions;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

@Slf4j
abstract class AbstractGrpcAgent extends AbstractAgentCode {
    protected static final ObjectMapper MAPPER = new ObjectMapper();
    protected ManagedChannel channel;

    // For each schema sent, we increment the schemaId
    private final AtomicInteger schemaId = new AtomicInteger(0);

    // Schemas sent to the server
    private final Map<Object, Integer> schemaIds = new ConcurrentHashMap<>();

    // Schemas received from the server
    protected final Map<Integer, Object> serverSchemas = new ConcurrentHashMap<>();

    protected AgentServiceGrpc.AgentServiceBlockingStub blockingStub;

    protected final AtomicBoolean restarting = new AtomicBoolean(false);

    @Getter protected volatile boolean startFailedButDevelopmentMode = false;
    protected AgentServiceGrpc.AgentServiceStub asyncStub;

    protected CompletableFuture<StreamObserver<TopicProducerWriteResult>>
            topicProducerWriteResults = CompletableFuture.completedFuture(null);

    private final Map<String, TopicProducer> topicProducers = new ConcurrentHashMap<>();

    protected record GrpcAgentRecord(
            Long id,
            Object key,
            Object value,
            String origin,
            Long timestamp,
            Collection<ai.langstream.api.runner.code.Header> headers)
            implements ai.langstream.api.runner.code.Record {}

    public AbstractGrpcAgent() {}

    public AbstractGrpcAgent(ManagedChannel channel) {
        this.channel = channel;
    }

    public abstract void onNewSchemaToSend(Schema schema);

    @Override
    public void start() throws Exception {
        if (channel == null) {
            throw new IllegalStateException("Channel not initialized");
        }
        blockingStub =
                AgentServiceGrpc.newBlockingStub(channel)
                        .withMaxInboundMessageSize(Integer.MAX_VALUE)
                        .withMaxOutboundMessageSize(Integer.MAX_VALUE)
                        .withDeadlineAfter(30, TimeUnit.SECONDS);
        asyncStub =
                AgentServiceGrpc.newStub(channel)
                        .withWaitForReady()
                        .withMaxInboundMessageSize(Integer.MAX_VALUE)
                        .withMaxOutboundMessageSize(Integer.MAX_VALUE);

        topicProducerWriteResults = new CompletableFuture<>();
        topicProducerWriteResults.complete(
                asyncStub.getTopicProducerRecords(
                        new StreamObserver<>() {
                            @Override
                            public void onNext(TopicProducerResponse topicProducerResponse) {
                                try {
                                    if (topicProducerResponse.hasSchema()) {
                                        serverSchemas.put(
                                                topicProducerResponse.getSchema().getSchemaId(),
                                                new org.apache.avro.Schema.Parser()
                                                        .parse(
                                                                topicProducerResponse
                                                                        .getSchema()
                                                                        .getValue()
                                                                        .toStringUtf8()));
                                    }
                                    if (topicProducerResponse.hasRecord()
                                            && !"".equals(topicProducerResponse.getTopic())) {
                                        TopicProducer topicProducer =
                                                topicProducers.computeIfAbsent(
                                                        topicProducerResponse.getTopic(),
                                                        topic -> {
                                                            TopicProducer tp =
                                                                    agentContext
                                                                            .getTopicConnectionProvider()
                                                                            .createProducer(
                                                                                    agentContext
                                                                                            .getGlobalAgentId(),
                                                                                    topic,
                                                                                    Map.of());
                                                            tp.start();
                                                            return tp;
                                                        });
                                        topicProducer
                                                .write(fromGrpc(topicProducerResponse.getRecord()))
                                                .whenComplete(
                                                        (r, e) -> {
                                                            if (e != null) {
                                                                log.error(
                                                                        "Error writing record", e);
                                                                sendTopicProducerWriteResult(
                                                                        TopicProducerWriteResult
                                                                                .newBuilder()
                                                                                .setError(
                                                                                        e
                                                                                                .getMessage()));
                                                            } else {
                                                                sendTopicProducerWriteResult(
                                                                        TopicProducerWriteResult
                                                                                .newBuilder()
                                                                                .setRecordId(
                                                                                        topicProducerResponse
                                                                                                .getRecord()
                                                                                                .getRecordId()));
                                                            }
                                                        });
                                    }
                                } catch (Exception e) {
                                    agentContext.criticalFailure(
                                            new RuntimeException(
                                                    "getTopicProducerRecords: Error while processing TopicProducerResponse: %s"
                                                            .formatted(e.getMessage()),
                                                    e));
                                }
                            }

                            @Override
                            public void onError(Throwable throwable) {
                                if (!restarting.get()) {
                                    agentContext.criticalFailure(
                                            new RuntimeException(
                                                    "getTopicProducerRecords: gRPC server sent error: %s"
                                                            .formatted(throwable.getMessage()),
                                                    throwable));
                                } else {
                                    log.info(
                                            "getTopicProducerRecords: ignoring error during restart {}",
                                            throwable + "");
                                }
                            }

                            @Override
                            public void onCompleted() {
                                if (!restarting.get()) {
                                    agentContext.criticalFailure(
                                            new RuntimeException(
                                                    "getTopicProducerRecords: gRPC server completed the stream unexpectedly"));
                                } else {
                                    log.info(
                                            "getTopicProducerRecords: ignoring error server stop during restart");
                                }
                            }
                        }));
    }

    private synchronized void sendTopicProducerWriteResult(
            TopicProducerWriteResult.Builder result) {
        try {
            topicProducerWriteResults.get().onNext(result.build());
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Map<String, Object> buildAdditionalInfo() {
        try {
            return MAPPER.readValue(
                    blockingStub.agentInfo(Empty.getDefaultInstance()).getJsonInfo(), Map.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    protected synchronized void stopBeforeRestart() throws Exception {
        restarting.set(true);
        StreamObserver<TopicProducerWriteResult> topicProducerWriteResultStreamObserver =
                topicProducerWriteResults.get();
        if (topicProducerWriteResultStreamObserver != null) {
            try {
                topicProducerWriteResultStreamObserver.onCompleted();
            } catch (IllegalStateException e) {
                log.info("Ignoring error while stopping {}", e + "");
            }
        }
        stopChannel(false);
    }

    public void stopChannel(boolean wait) throws Exception {
        ManagedChannel currentChannel;
        synchronized (this) {
            currentChannel = channel;
        }
        if (currentChannel != null) {
            ManagedChannel shutdown = currentChannel.shutdown();
            if (wait) {
                shutdown.awaitTermination(10, TimeUnit.SECONDS);
            }
        }
    }

    @Override
    public synchronized void close() throws Exception {
        stopBeforeRestart();
        stopChannel(true);
        for (TopicProducer topicProducer : topicProducers.values()) {
            topicProducer.close();
        }
        topicProducers.clear();
    }

    protected Object fromGrpc(Value value) throws IOException {
        if (value == null) {
            return null;
        }
        return switch (value.getTypeOneofCase()) {
            case BYTES_VALUE -> value.getBytesValue().toByteArray();
            case BOOLEAN_VALUE -> value.getBooleanValue();
            case STRING_VALUE -> value.getStringValue();
            case BYTE_VALUE -> (byte) value.getByteValue();
            case SHORT_VALUE -> (short) value.getShortValue();
            case INT_VALUE -> value.getIntValue();
            case LONG_VALUE -> value.getLongValue();
            case FLOAT_VALUE -> value.getFloatValue();
            case DOUBLE_VALUE -> value.getDoubleValue();
            case JSON_VALUE -> value.getJsonValue();
            case AVRO_VALUE -> {
                Object serverSchema = serverSchemas.get(value.getSchemaId());
                if (serverSchema instanceof org.apache.avro.Schema schema) {
                    yield deserializeGenericRecord(schema, value.getAvroValue().toByteArray());
                } else {
                    log.error("Unknown schema id {}", value.getSchemaId());
                    throw new RuntimeException("Unknown schema id " + value.getSchemaId());
                }
            }
            case TYPEONEOF_NOT_SET -> null;
        };
    }

    protected GrpcAgentRecord fromGrpc(Record record) throws IOException {
        List<ai.langstream.api.runner.code.Header> headers = new ArrayList<>();
        for (Header header : record.getHeadersList()) {
            headers.add(fromGrpc(header));
        }
        return new GrpcAgentRecord(
                record.getRecordId(),
                fromGrpc(record.getKey()),
                fromGrpc(record.getValue()),
                record.getOrigin().isEmpty() ? null : record.getOrigin(),
                record.hasTimestamp() ? record.getTimestamp() : null,
                headers);
    }

    protected SimpleRecord.SimpleHeader fromGrpc(Header header) throws IOException {
        return SimpleRecord.SimpleHeader.of(header.getName(), fromGrpc(header.getValue()));
    }

    protected Record.Builder toGrpc(ai.langstream.api.runner.code.Record record)
            throws IOException {
        Record.Builder recordBuilder = Record.newBuilder();
        if (record.value() != null) {
            recordBuilder.setValue(toGrpc(record.value()));
        }

        if (record.key() != null) {
            recordBuilder.setKey(toGrpc(record.key()));
        }

        if (record.origin() != null) {
            recordBuilder.setOrigin(record.origin());
        }

        if (record.timestamp() != null) {
            recordBuilder.setTimestamp(record.timestamp());
        }

        if (record.headers() != null) {
            for (ai.langstream.api.runner.code.Header h : record.headers()) {
                Header.Builder headerBuilder = recordBuilder.addHeadersBuilder().setName(h.key());
                if (h.value() != null) {
                    headerBuilder.setValue(toGrpc(h.value()));
                }
            }
        }
        return recordBuilder;
    }

    protected Value toGrpc(Object obj) throws IOException {
        if (obj == null) {
            return null;
        }
        Value.Builder valueBuilder = Value.newBuilder();
        if (obj instanceof String value) {
            valueBuilder.setStringValue(value);
        } else if (obj instanceof byte[] value) {
            valueBuilder.setBytesValue(ByteString.copyFrom((value)));
        } else if (obj instanceof Boolean value) {
            valueBuilder.setBooleanValue(value);
        } else if (obj instanceof Byte value) {
            valueBuilder.setByteValue(value.intValue());
        } else if (obj instanceof Short value) {
            valueBuilder.setShortValue(value.intValue());
        } else if (obj instanceof Integer value) {
            valueBuilder.setIntValue(value);
        } else if (obj instanceof Long value) {
            valueBuilder.setLongValue(value);
        } else if (obj instanceof Float value) {
            valueBuilder.setFloatValue(value);
        } else if (obj instanceof Double value) {
            valueBuilder.setDoubleValue(value);
        } else if (obj instanceof JsonNode value) {
            valueBuilder.setJsonValue(value.toString());
        } else if (obj instanceof GenericRecord genericRecord) {
            org.apache.avro.Schema schema = genericRecord.getSchema();
            Integer schemaId =
                    schemaIds.computeIfAbsent(
                            schema,
                            s -> {
                                int sId = this.schemaId.incrementAndGet();
                                onNewSchemaToSend(
                                        Schema.newBuilder()
                                                .setValue(
                                                        ByteString.copyFromUtf8(schema.toString()))
                                                .setSchemaId(sId)
                                                .build());
                                return sId;
                            });

            valueBuilder.setSchemaId(schemaId);
            valueBuilder.setAvroValue(ByteString.copyFrom(serializeGenericRecord(genericRecord)));
        } else {
            throw new IllegalArgumentException("Unsupported type " + obj.getClass());
        }
        return valueBuilder.build();
    }

    private static byte[] serializeGenericRecord(GenericRecord record) throws IOException {
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());
        // enable Decimal conversion, otherwise attempting to serialize java.math.BigDecimal will
        // throw ClassCastException.
        writer.getData().addLogicalTypeConversion(new Conversions.DecimalConversion());
        ByteArrayOutputStream oo = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(oo, null);
        writer.write(record, encoder);
        return oo.toByteArray();
    }

    private static GenericRecord deserializeGenericRecord(
            org.apache.avro.Schema schema, byte[] data) throws IOException {
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        reader.getData().addLogicalTypeConversion(new Conversions.DecimalConversion());
        Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
        return reader.read(null, decoder);
    }

    @Override
    @SneakyThrows
    public void restart() throws Exception {
        log.info("Restarting...");
        try {
            stopBeforeRestart();
            try {
                start();
            } catch (Exception error) {
                if (ConfigurationUtils.isDevelopmentMode()) {
                    log.info(
                            "The Python agent failed to restart, ignoring. Maybe there is a syntax error",
                            error);
                    startFailedButDevelopmentMode = true;
                    try {
                        stopBeforeRestart();
                    } catch (Exception ignored) {
                    }
                } else {
                    throw error;
                }
            }
            log.info("Restart completed");
        } catch (Throwable error) {
            log.error("Error while restarting", error);
            throw error;
        }
    }
}
