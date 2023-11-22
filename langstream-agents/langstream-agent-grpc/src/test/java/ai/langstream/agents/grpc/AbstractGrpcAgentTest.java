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

import ai.langstream.api.runner.code.AgentContext;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.topics.TopicAdmin;
import ai.langstream.api.runner.topics.TopicConnectionProvider;
import ai.langstream.api.runner.topics.TopicConsumer;
import ai.langstream.api.runner.topics.TopicProducer;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Conversions;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Slf4j
public class AbstractGrpcAgentTest {

    private Server server;
    private ManagedChannel channel;
    private GrpcAgentProcessor processor;

    private final AgentServiceGrpc.AgentServiceImplBase testProcessorService =
            new AgentServiceGrpc.AgentServiceImplBase() {

                @Override
                public void agentInfo(
                        Empty request, StreamObserver<InfoResponse> responseObserver) {
                    responseObserver.onNext(
                            InfoResponse.newBuilder()
                                    .setJsonInfo("{\"test-info-key\": \"test-info-value\"}")
                                    .build());
                    responseObserver.onCompleted();
                }

                @Override
                public StreamObserver<TopicProducerWriteResult> getTopicProducerRecords(
                        StreamObserver<TopicProducerResponse> responseObserver) {
                    org.apache.avro.Schema schema =
                            SchemaBuilder.record("testRecord")
                                    .fields()
                                    .name("testField")
                                    .type()
                                    .stringType()
                                    .noDefault()
                                    .endRecord();
                    GenericData.Record avroRecord = new GenericData.Record(schema);
                    avroRecord.put("testField", "test-string");

                    responseObserver.onNext(
                            TopicProducerResponse.newBuilder()
                                    .setSchema(
                                            Schema.newBuilder()
                                                    .setSchemaId(123)
                                                    .setValue(
                                                            ByteString.copyFromUtf8(
                                                                    schema.toString()))
                                                    .build())
                                    .build());

                    try {
                        responseObserver.onNext(
                                TopicProducerResponse.newBuilder()
                                        .setTopic("test-topic")
                                        .setRecord(
                                                ai.langstream.agents.grpc.Record.newBuilder()
                                                        .setRecordId(42)
                                                        .setValue(
                                                                Value.newBuilder()
                                                                        .setSchemaId(123)
                                                                        .setAvroValue(
                                                                                ByteString.copyFrom(
                                                                                        serializeGenericRecord(
                                                                                                avroRecord)))))
                                        .build());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    return new StreamObserver<>() {

                        @Override
                        public void onNext(TopicProducerWriteResult topicProducerWriteResult) {
                            if (topicProducerWriteResult.getError().equals("test-error")) {
                                responseObserver.onError(
                                        Status.INTERNAL
                                                .withDescription("test-error")
                                                .asRuntimeException());
                            } else if (topicProducerWriteResult
                                    .getError()
                                    .equals("test-complete")) {
                                responseObserver.onCompleted();
                            } else if (topicProducerWriteResult.getRecordId() == 42) {
                                responseObserver.onNext(
                                        TopicProducerResponse.newBuilder()
                                                .setTopic("test-topic")
                                                .setRecord(
                                                        ai.langstream.agents.grpc.Record
                                                                .newBuilder()
                                                                .setRecordId(43)
                                                                .setValue(
                                                                        Value.newBuilder()
                                                                                .setStringValue(
                                                                                        "test-value2")))
                                                .build());
                            }
                        }

                        @Override
                        public void onError(Throwable throwable) {}

                        @Override
                        public void onCompleted() {
                            responseObserver.onCompleted();
                        }
                    };
                }

                @Override
                public StreamObserver<ProcessorRequest> process(
                        StreamObserver<ProcessorResponse> responseObserver) {
                    return new StreamObserver<>() {
                        @Override
                        public void onNext(ProcessorRequest processorRequest) {}

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
                        .addService(testProcessorService)
                        .build()
                        .start();

        channel = InProcessChannelBuilder.forName(serverName).build();
        processor = new GrpcAgentProcessor(channel);
    }

    @AfterEach
    public void tearDown() throws Exception {
        processor.close();
        channel.shutdownNow();
        server.shutdownNow();
        channel.awaitTermination(30, TimeUnit.SECONDS);
        server.awaitTermination(30, TimeUnit.SECONDS);
    }

    @Test
    void testInfo() throws Exception {
        startProcessor(new TestAgentContextSuccessful());
        Map<String, Object> info = processor.buildAdditionalInfo();
        assertEquals("test-info-value", info.get("test-info-key"));
    }

    @Test
    void testTopicProducerSuccess() throws Exception {
        TestAgentContext context = new TestAgentContextSuccessful();
        startProcessor(context);
        LinkedBlockingQueue<Record> recordsToWrite = context.recordsToWrite;
        assertEquals(
                "{\"testField\": \"test-string\"}",
                recordsToWrite.poll(5, TimeUnit.SECONDS).value().toString());
        assertEquals("test-value2", recordsToWrite.poll(5, TimeUnit.SECONDS).value().toString());
    }

    @Test
    void testTopicProducerError() throws Exception {
        TestAgentContext context = new TestAgentContextFailure();
        startProcessor(context);
        assertEquals(
                "getTopicProducerRecords: gRPC server sent error: INTERNAL: test-error",
                context.failure.get(15, TimeUnit.SECONDS).getMessage());
    }

    @Test
    void testTopicProducerComplete() throws Exception {
        TestAgentContextCompleting context = new TestAgentContextCompleting();
        startProcessor(context);
        assertEquals(
                "getTopicProducerRecords: gRPC server completed the stream unexpectedly",
                context.failure.get(5, TimeUnit.SECONDS).getMessage());
    }

    private void startProcessor(AgentContext context) throws Exception {
        processor.setContext(context);
        processor.start();
    }

    abstract static class TestAgentContext implements AgentContext {
        protected final LinkedBlockingQueue<Record> recordsToWrite = new LinkedBlockingQueue<>();
        protected final CompletableFuture<Throwable> failure = new CompletableFuture<>();

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

        protected abstract CompletableFuture<?> writeRecord(Record record);

        @Override
        public TopicConnectionProvider getTopicConnectionProvider() {
            return new TopicConnectionProvider() {
                @Override
                public TopicProducer createProducer(
                        String agentId, String topic, Map<String, Object> config) {
                    return new TopicProducer() {
                        @Override
                        public CompletableFuture<?> write(
                                ai.langstream.api.runner.code.Record record) {
                            if (topic.equals("test-topic")) {
                                return writeRecord(record);
                            }
                            return CompletableFuture.completedFuture(null);
                        }

                        @Override
                        public long getTotalIn() {
                            return 0;
                        }
                    };
                }
            };
        }

        @Override
        public void criticalFailure(Throwable error) {
            log.info("TestAgentContext critical failure", error);
            failure.complete(error);
        }

        @Override
        public Path getCodeDirectory() {
            return null;
        }
    }

    static class TestAgentContextSuccessful extends TestAgentContext {
        @Override
        protected CompletableFuture<?> writeRecord(Record record) {
            recordsToWrite.add(record);
            return CompletableFuture.completedFuture(null);
        }
    }

    static class TestAgentContextFailure extends TestAgentContext {
        @Override
        protected CompletableFuture<?> writeRecord(Record record) {
            return CompletableFuture.failedFuture(new RuntimeException("test-error"));
        }
    }

    static class TestAgentContextCompleting extends TestAgentContext {
        @Override
        protected CompletableFuture<?> writeRecord(Record record) {
            return CompletableFuture.failedFuture(new RuntimeException("test-complete"));
        }
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
}
