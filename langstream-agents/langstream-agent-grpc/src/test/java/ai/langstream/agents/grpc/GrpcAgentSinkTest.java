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
import ai.langstream.api.runner.code.SimpleRecord;
import ai.langstream.api.runner.topics.TopicAdmin;
import ai.langstream.api.runner.topics.TopicConnectionProvider;
import ai.langstream.api.runner.topics.TopicConsumer;
import ai.langstream.api.runner.topics.TopicProducer;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class GrpcAgentSinkTest {
    private Server server;
    private ManagedChannel channel;
    private final TestSinkService testSinkService = new TestSinkService();
    private GrpcAgentSink sink;
    private TestAgentContext context;

    @BeforeEach
    public void setUp() throws Exception {
        String serverName = InProcessServerBuilder.generateName();
        server =
                InProcessServerBuilder.forName(serverName)
                        .directExecutor()
                        .addService(testSinkService)
                        .build()
                        .start();

        channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        sink = new GrpcAgentSink(channel);
        context = new TestAgentContext();
        sink.setContext(context);
        sink.start();
    }

    @AfterEach
    public void tearDown() throws Exception {
        sink.close();
        channel.shutdownNow();
        server.shutdownNow();
        channel.awaitTermination(30, TimeUnit.SECONDS);
        server.awaitTermination(30, TimeUnit.SECONDS);
    }

    @Test
    void testWriteError() throws Exception {
        try {
            sink.write(SimpleRecord.builder().origin("failing-record").build())
                    .get(5, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            assertEquals("test-error", e.getCause().getMessage());
        }
    }

    @Test
    void testSinkGrpcError() throws Exception {
        sink.write(SimpleRecord.builder().origin("failing-server").build());
        assertEquals(
                "gRPC server sent error: UNKNOWN",
                context.failure.get(1, TimeUnit.SECONDS).getMessage());
    }

    @Test
    void testSinkGrpcCompletedUnexpectedly() throws Exception {
        sink.write(SimpleRecord.builder().origin("completing-server").build());
        assertEquals(
                "gRPC server completed the stream unexpectedly",
                context.failure.get(1, TimeUnit.SECONDS).getMessage());
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

        sink.write(SimpleRecord.of(null, avroRecord)).get(5, TimeUnit.SECONDS);

        GenericRecord writtenRecord = testSinkService.avroRecords.poll(5, TimeUnit.SECONDS);
        assertEquals("test-string", writtenRecord.get("testField").toString());
    }

    static class TestSinkService extends AgentServiceGrpc.AgentServiceImplBase {

        private final Map<Integer, Schema> schemas = new ConcurrentHashMap<>();
        private final LinkedBlockingQueue<GenericRecord> avroRecords = new LinkedBlockingQueue<>();

        @Override
        public StreamObserver<SinkRequest> write(StreamObserver<SinkResponse> responseObserver) {
            return new StreamObserver<>() {
                @Override
                public void onNext(SinkRequest request) {
                    if (request.hasSchema()) {
                        Schema schema =
                                new Schema.Parser()
                                        .parse(request.getSchema().getValue().toStringUtf8());
                        schemas.put(request.getSchema().getSchemaId(), schema);
                    }
                    if (request.hasRecord()) {
                        ai.langstream.agents.grpc.Record record = request.getRecord();
                        Value value = record.getValue();
                        if (value.hasAvroValue()) {
                            Schema schema = schemas.get(value.getSchemaId());
                            try {
                                GenericRecord genericRecord =
                                        deserializeGenericRecord(
                                                schema, value.getAvroValue().toByteArray());
                                avroRecords.add(genericRecord);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        if (record.getOrigin().equals("failing-record")) {
                            responseObserver.onNext(
                                    SinkResponse.newBuilder()
                                            .setRecordId(record.getRecordId())
                                            .setError("test-error")
                                            .build());
                        } else if (record.getOrigin().equals("failing-server")) {
                            responseObserver.onError(new RuntimeException("test-error"));
                        } else if (record.getOrigin().equals("completing-server")) {
                            responseObserver.onCompleted();
                        } else {
                            responseObserver.onNext(
                                    SinkResponse.newBuilder()
                                            .setRecordId(record.getRecordId())
                                            .build());
                        }
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
    }

    private static GenericRecord deserializeGenericRecord(
            org.apache.avro.Schema schema, byte[] data) throws IOException {
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        reader.getData().addLogicalTypeConversion(new Conversions.DecimalConversion());
        Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
        return reader.read(null, decoder);
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
