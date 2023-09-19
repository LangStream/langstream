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
import ai.langstream.api.runner.code.AgentContext;
import ai.langstream.api.runner.code.AgentProcessor;
import ai.langstream.api.runner.code.RecordSink;
import ai.langstream.api.runner.code.SimpleRecord;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
public class GrpcAgentProcessor extends AbstractAgentCode implements AgentProcessor {
    protected static final ObjectMapper MAPPER = new ObjectMapper();
    protected ManagedChannel channel;
    private StreamObserver<ProcessorRequest> request;
    private RecordSink sink;

    // For each record sent, we increment the recordId
    private final AtomicLong recordId = new AtomicLong(0);

    // For each record sent, we store the record and the sink to which the result should be emitted
    private final Map<Long, RecordAndSink> sourceRecords = new ConcurrentHashMap<>();

    // For each schema sent, we increment the schemaId
    private final AtomicInteger schemaId = new AtomicInteger(0);

    // Schemas sent to the server
    private final Map<Object, Integer> schemaIds = new ConcurrentHashMap<>();

    // Schemas received from the server
    private final Map<Integer, Object> serverSchemas = new ConcurrentHashMap<>();

    private final StreamObserver<ProcessorResponse> responseObserver = getResponseObserver();
    protected AgentContext agentContext;
    protected AgentServiceGrpc.AgentServiceBlockingStub blockingStub;

    private record RecordAndSink(
            ai.langstream.api.runner.code.Record sourceRecord, RecordSink sink) {}

    public GrpcAgentProcessor() {}

    public GrpcAgentProcessor(ManagedChannel channel) {
        this.channel = channel;
    }

    @Override
    public void start() throws Exception {
        if (channel == null) {
            throw new IllegalStateException("Channel not initialized");
        }
        blockingStub = AgentServiceGrpc.newBlockingStub(channel);
        request = AgentServiceGrpc.newStub(channel).withWaitForReady().process(responseObserver);
    }

    @Override
    public void setContext(AgentContext context) throws Exception {
        this.agentContext = context;
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

    @Override
    public synchronized void process(
            List<ai.langstream.api.runner.code.Record> records, RecordSink recordSink) {
        if (sink == null) {
            sink = recordSink;
        }

        ProcessorRequest.Builder requestBuilder = ProcessorRequest.newBuilder();
        for (ai.langstream.api.runner.code.Record record : records) {
            long rId = recordId.incrementAndGet();
            try {
                Record.Builder recordBuilder = requestBuilder.addRecordsBuilder().setRecordId(rId);

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
                        Header.Builder headerBuilder =
                                recordBuilder.addHeadersBuilder().setName(h.key());
                        if (h.value() != null) {
                            headerBuilder.setValue(toGrpc(h.value()));
                        }
                    }
                }
                sourceRecords.put(rId, new RecordAndSink(record, recordSink));
            } catch (Exception e) {
                recordSink.emit(new SourceRecordAndResult(record, null, e));
            }
        }
        if (requestBuilder.getRecordsCount() > 0) {
            request.onNext(requestBuilder.build());
        }
    }

    @Override
    public synchronized void close() throws Exception {
        if (request != null) {
            request.onCompleted();
        }
        if (channel != null) {
            channel.shutdown();
        }
    }

    private Object fromGrpc(Value value) throws IOException {
        if (value == null) {
            return null;
        }
        return switch (value.getTypeOneofCase()) {
            case BYTESVALUE -> value.getBytesValue().toByteArray();
            case BOOLEANVALUE -> value.getBooleanValue();
            case STRINGVALUE -> value.getStringValue();
            case BYTEVALUE -> (byte) value.getByteValue();
            case SHORTVALUE -> (short) value.getShortValue();
            case INTVALUE -> value.getIntValue();
            case LONGVALUE -> value.getLongValue();
            case FLOATVALUE -> value.getFloatValue();
            case DOUBLEVALUE -> value.getDoubleValue();
            case JSONVALUE -> value.getJsonValue();
            case AVROVALUE -> {
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

    private SourceRecordAndResult fromGrpc(
            ai.langstream.api.runner.code.Record sourceRecord, ProcessorResult result)
            throws IOException {
        List<ai.langstream.api.runner.code.Record> resultRecords = new ArrayList<>();
        if (result.hasError()) {
            // TODO: specialize exception ?
            return new SourceRecordAndResult(
                    sourceRecord, null, new RuntimeException(result.getError()));
        }
        for (Record record : result.getRecordsList()) {
            resultRecords.add(fromGrpc(record));
        }
        return new SourceRecordAndResult(sourceRecord, resultRecords, null);
    }

    private SimpleRecord fromGrpc(Record record) throws IOException {
        List<ai.langstream.api.runner.code.Header> headers = new ArrayList<>();
        for (Header header : record.getHeadersList()) {
            headers.add(fromGrpc(header));
        }
        SimpleRecord.SimpleRecordBuilder result =
                SimpleRecord.builder()
                        .key(fromGrpc(record.getKey()))
                        .value(fromGrpc(record.getValue()))
                        .headers(headers);

        if (!record.getOrigin().isEmpty()) {
            result.origin(record.getOrigin());
        }

        if (record.hasTimestamp()) {
            result.timestamp(record.getTimestamp());
        }

        return result.build();
    }

    private ai.langstream.api.runner.code.Header fromGrpc(Header header) throws IOException {
        return SimpleRecord.SimpleHeader.of(header.getName(), fromGrpc(header.getValue()));
    }

    private Value toGrpc(Object obj) throws IOException {
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
                                request.onNext(
                                        ProcessorRequest.newBuilder()
                                                .setSchema(
                                                        Schema.newBuilder()
                                                                .setValue(
                                                                        ByteString.copyFromUtf8(
                                                                                schema.toString()))
                                                                .setSchemaId(sId))
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

    private StreamObserver<ProcessorResponse> getResponseObserver() {
        return new StreamObserver<>() {
            @Override
            public void onNext(ProcessorResponse response) {
                if (response.hasSchema()) {
                    org.apache.avro.Schema schema =
                            new org.apache.avro.Schema.Parser()
                                    .parse(response.getSchema().getValue().toStringUtf8());
                    serverSchemas.put(response.getSchema().getSchemaId(), schema);
                }
                response.getResultsList()
                        .forEach(
                                result -> {
                                    RecordAndSink recordAndSink =
                                            sourceRecords.remove(result.getRecordId());
                                    if (recordAndSink == null) {
                                        agentContext.criticalFailure(
                                                new RuntimeException(
                                                        "Received unknown record id "
                                                                + result.getRecordId()));
                                    } else {
                                        try {
                                            recordAndSink
                                                    .sink()
                                                    .emit(
                                                            fromGrpc(
                                                                    recordAndSink.sourceRecord(),
                                                                    result));
                                        } catch (Exception e) {
                                            agentContext.criticalFailure(
                                                    new RuntimeException(
                                                            "Error while processing record %s: %s"
                                                                    .formatted(
                                                                            result.getRecordId(),
                                                                            e.getMessage()),
                                                            e));
                                        }
                                    }
                                });
            }

            @Override
            public void onError(Throwable throwable) {
                agentContext.criticalFailure(
                        new RuntimeException(
                                "gRPC server sent error: %s".formatted(throwable.getMessage()),
                                throwable));
            }

            @Override
            public void onCompleted() {
                agentContext.criticalFailure(
                        new RuntimeException("gRPC server completed the stream unexpectedly"));
            }
        };
    }
}
