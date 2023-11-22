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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.langstream.api.runner.code.AgentContext;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SimpleRecord;
import ai.langstream.api.runner.code.SimpleRecord.SimpleHeader;
import ai.langstream.api.runner.topics.TopicAdmin;
import ai.langstream.api.runner.topics.TopicConnectionProvider;
import ai.langstream.api.runner.topics.TopicConsumer;
import ai.langstream.api.runner.topics.TopicProducer;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

public class GrpcAgentProcessorTest {
    private Server server;
    private ManagedChannel channel;
    private GrpcAgentProcessor processor;
    private TestAgentContext context;
    private final AtomicInteger schemaCounter = new AtomicInteger(0);

    private final AgentServiceGrpc.AgentServiceImplBase testProcessorService =
            new AgentServiceGrpc.AgentServiceImplBase() {

                @Override
                public StreamObserver<ProcessorRequest> process(
                        StreamObserver<ProcessorResponse> response) {
                    return new StreamObserver<>() {
                        @Override
                        public void onNext(ProcessorRequest request) {
                            ProcessorResponse.Builder resp = ProcessorResponse.newBuilder();
                            if (request.hasSchema()) {
                                schemaCounter.incrementAndGet();
                                resp.setSchema(request.getSchema());
                            }
                            if (request.getRecordsCount() > 0) {
                                for (ai.langstream.agents.grpc.Record record :
                                        request.getRecordsList()) {
                                    ProcessorResult.Builder resultBuilder =
                                            resp.addResultsBuilder()
                                                    .setRecordId(record.getRecordId());
                                    if (record.getOrigin().equals("failing-origin")) {
                                        resultBuilder.setError("test-error");
                                    } else if (record.getOrigin().equals("failing-server")) {
                                        response.onError(
                                                Status.INTERNAL
                                                        .withDescription("server error")
                                                        .asException());
                                        return;
                                    } else if (record.getOrigin().equals("completing-server")) {
                                        response.onCompleted();
                                        return;
                                    } else if (record.getOrigin().equals("wrong-record-id")) {
                                        resultBuilder.setRecordId(record.getRecordId() + 1);
                                    } else {
                                        if (record.getOrigin().equals("wrong-schema-id")) {
                                            resultBuilder
                                                    .addRecordsBuilder()
                                                    .getValueBuilder()
                                                    .setSchemaId(1)
                                                    .setAvroValue(ByteString.EMPTY);
                                        } else {
                                            resultBuilder.addRecords(record);
                                        }
                                    }
                                }
                            }
                            response.onNext(resp.build());
                        }

                        @Override
                        public void onError(Throwable throwable) {}

                        @Override
                        public void onCompleted() {
                            response.onCompleted();
                        }
                    };
                }

                @Override
                public StreamObserver<TopicProducerWriteResult> getTopicProducerRecords(
                        StreamObserver<TopicProducerResponse> responseObserver) {
                    return new StreamObserver<>() {
                        @Override
                        public void onNext(TopicProducerWriteResult topicProducerWriteResult) {}

                        @Override
                        public void onError(Throwable throwable) {}

                        @Override
                        public void onCompleted() {
                            responseObserver.onCompleted();
                        }
                    };
                }
            };

    @BeforeEach
    public void setUp() throws Exception {
        String serverName = InProcessServerBuilder.generateName();
        server =
                InProcessServerBuilder.forName(serverName)
                        .directExecutor()
                        .addService(testProcessorService)
                        .build()
                        .start();

        channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        processor = new GrpcAgentProcessor(channel);
        context = new TestAgentContext();
        processor.setContext(context);
        processor.start();
        schemaCounter.set(0);
    }

    @AfterEach
    public void tearDown() throws Exception {
        channel.shutdownNow();
        server.shutdownNow();
        channel.awaitTermination(30, TimeUnit.SECONDS);
        server.awaitTermination(30, TimeUnit.SECONDS);
    }

    private static Stream<Arguments> primitives() {
        return Stream.of(
                Arguments.of("test-value", "test-key", "test-header"),
                Arguments.of(true, true, true),
                Arguments.of(
                        "test-value".getBytes(StandardCharsets.UTF_8),
                        "test-key".getBytes(StandardCharsets.UTF_8),
                        "test-header".getBytes(StandardCharsets.UTF_8)),
                Arguments.of((byte) 42, (byte) 43, (byte) 44),
                Arguments.of((short) 42, (short) 43, (short) 44),
                Arguments.of(42, 43, 44),
                Arguments.of(42L, 43L, 44L),
                Arguments.of(42.0f, 43.0f, 44.0f),
                Arguments.of(42.0, 43.0, 44.0),
                Arguments.of(null, null, null));
    }

    @ParameterizedTest
    @MethodSource("primitives")
    void testProcess(Object value, Object key, Object header) throws Exception {
        Record inputRecord =
                SimpleRecord.builder()
                        .value(value)
                        .key(key)
                        .origin("test-origin")
                        .headers(List.of(SimpleHeader.of("test-header-key", header)))
                        .timestamp(42L)
                        .build();
        assertProcessSuccessful(processor, inputRecord);
        assertProcessSuccessful(processor, inputRecord);
    }

    @Test
    void testEmpty() throws Exception {
        assertProcessSuccessful(processor, SimpleRecord.builder().build());
    }

    @Test
    void testFailingRecord() throws Exception {
        Record inputRecord = SimpleRecord.builder().origin("failing-origin").build();
        CompletableFuture<Void> op = new CompletableFuture<>();
        processor.process(
                List.of(inputRecord),
                result -> {
                    try {
                        assertSame(inputRecord, result.sourceRecord());
                        assertInstanceOf(RuntimeException.class, result.error());
                        assertEquals("test-error", result.error().getMessage());
                        assertTrue(result.resultRecords().isEmpty());
                    } catch (Throwable t) {
                        op.completeExceptionally(t);
                    }
                    op.complete(null);
                });
        op.get(5, TimeUnit.SECONDS);
    }

    @ParameterizedTest
    @CsvSource({
        "failing-server,gRPC server sent error: INTERNAL: server error",
        "completing-server,gRPC server completed the stream unexpectedly",
        "wrong-record-id,GrpcAgentProcessor error while processing record: Received unknown record id 2",
        "wrong-schema-id,GrpcAgentProcessor error while processing record: Unknown schema id 1"
    })
    void testServerError(String origin, String error) throws Exception {
        Record inputRecord = SimpleRecord.builder().origin(origin).build();

        processor.process(List.of(inputRecord), result -> {});

        assertEquals(error, context.failure.get(1, TimeUnit.SECONDS).getMessage());
    }

    @Test
    void testAvroAndSchema() throws Exception {
        Schema schema =
                SchemaBuilder.record("testRecord")
                        .fields()
                        .name("testField")
                        .type()
                        .stringType()
                        .noDefault()
                        .endRecord();
        GenericData.Record avroRecord = new GenericData.Record(schema);
        avroRecord.put("testField", "test-string");
        Record inputRecord =
                SimpleRecord.builder()
                        .value(avroRecord)
                        .key(avroRecord)
                        .headers(List.of(SimpleHeader.of("test-header", avroRecord)))
                        .build();
        // Check that the schema is sent only once
        assertProcessSuccessful(processor, inputRecord);
        assertProcessSuccessful(processor, inputRecord);
        assertEquals(1, schemaCounter.get());
    }

    private static void assertProcessSuccessful(GrpcAgentProcessor processor, Record inputRecord)
            throws ExecutionException, InterruptedException, TimeoutException {
        CompletableFuture<Void> op = new CompletableFuture<>();
        processor.process(
                List.of(inputRecord),
                result -> {
                    try {
                        assertSame(inputRecord, result.sourceRecord());
                        assertNull(result.error());
                        assertEquals(1, result.resultRecords().size());
                        Record outputRecord = result.resultRecords().get(0);
                        assertValueEquals(inputRecord.key(), outputRecord.key());
                        assertValueEquals(inputRecord.value(), outputRecord.value());
                        inputRecord
                                .headers()
                                .forEach(
                                        h ->
                                                assertValueEquals(
                                                        h.value(),
                                                        outputRecord.getHeader(h.key()).value()));
                        assertEquals(inputRecord.origin(), outputRecord.origin());
                        assertEquals(inputRecord.timestamp(), outputRecord.timestamp());
                    } catch (Throwable t) {
                        op.completeExceptionally(t);
                    }
                    op.complete(null);
                });
        op.get(5, TimeUnit.SECONDS);
    }

    private static void assertValueEquals(Object expected, Object actual) {
        if (expected instanceof byte[] exp && actual instanceof byte[] act) {
            assertEquals(
                    new String(exp, StandardCharsets.UTF_8),
                    new String(act, StandardCharsets.UTF_8));
        } else {
            assertEquals(expected, actual);
        }
    }

    static class TestAgentContext implements AgentContext {

        private final CompletableFuture<Throwable> failure = new CompletableFuture<>();

        @Override
        public TopicConsumer getTopicConsumer() {
            return null;
        }

        @Override
        public TopicProducer getTopicProducer() {
            return null;
        }

        @Override
        public String getGlobalAgentId() {
            return null;
        }

        @Override
        public TopicAdmin getTopicAdmin() {
            return null;
        }

        @Override
        public TopicConnectionProvider getTopicConnectionProvider() {
            return null;
        }

        @Override
        public void criticalFailure(Throwable error) {
            failure.complete(error);
        }

        @Override
        public Path getCodeDirectory() {
            return null;
        }
    }
}
