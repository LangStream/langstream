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

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.langstream.api.runner.code.AgentContext;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.topics.TopicAdmin;
import ai.langstream.api.runner.topics.TopicConnectionProvider;
import ai.langstream.api.runner.topics.TopicConsumer;
import ai.langstream.api.runner.topics.TopicProducer;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Conversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class GrpcAgentSourceTest {
    private Server server;
    private ManagedChannel channel;

    private final TestSourceService testSourceService = new TestSourceService();

    @BeforeEach
    public void setUp() throws Exception {
        String serverName = InProcessServerBuilder.generateName();
        server =
                InProcessServerBuilder.forName(serverName)
                        .directExecutor()
                        .addService(testSourceService)
                        .build()
                        .start();

        channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
    }

    @AfterEach
    public void tearDown() throws Exception {
        channel.shutdownNow();
        server.shutdownNow();
        channel.awaitTermination(30, TimeUnit.SECONDS);
        server.awaitTermination(30, TimeUnit.SECONDS);
    }

    @Test
    void testCommit() throws Exception {
        GrpcAgentSource source = new GrpcAgentSource(channel);
        TestAgentContext context = new TestAgentContext();
        source.setContext(context);
        source.start();
        List<Record> read = readRecords(source, 3);
        source.commit(List.of(read.get(0)));
        assertFalse(context.failureCalled.await(1, TimeUnit.SECONDS));
        assertEquals(1, testSourceService.committedRecords.size());
        assertEquals(42, testSourceService.committedRecords.get(0));
        source.close();
    }

    @Test
    void testSourceGrpcError() throws Exception {
        GrpcAgentSource source = new GrpcAgentSource(channel);
        TestAgentContext context = new TestAgentContext();
        source.setContext(context);
        source.start();
        List<Record> read = readRecords(source, 3);
        source.commit(List.of(read.get(1)));
        assertTrue(context.failureCalled.await(1, TimeUnit.SECONDS));
        source.close();
    }

    @Test
    void testSourceGrpcCompletedUnexpectedly() throws Exception {
        GrpcAgentSource source = new GrpcAgentSource(channel);
        TestAgentContext context = new TestAgentContext();
        source.setContext(context);
        source.start();
        List<Record> read = readRecords(source, 3);
        source.commit(List.of(read.get(2)));
        assertTrue(context.failureCalled.await(1, TimeUnit.SECONDS));
        source.close();
    }

    @Test
    void testAvroAndSchema() throws Exception {
        GrpcAgentSource source = new GrpcAgentSource(channel);
        source.setContext(new TestAgentContext());
        source.start();
        List<Record> read = readRecords(source, 1);
        GenericRecord record = (GenericRecord) read.get(0).value();
        assertEquals("test-string", record.get("testField").toString());
        source.close();
    }

    @Test
    void testPermanentFailure() throws Exception {
        GrpcAgentSource source = new GrpcAgentSource(channel);
        source.setContext(new TestAgentContext());
        source.start();
        List<Record> read = readRecords(source, 1);
        source.permanentFailure(read.get(0), new RuntimeException("permanent-failure"));
        assertEquals(testSourceService.permanentFailure.getRecordId(), 42);
        assertEquals(testSourceService.permanentFailure.getErrorMessage(), "permanent-failure");
        source.close();
    }

    static List<Record> readRecords(GrpcAgentSource source, int numberOfRecords) {
        List<Record> read = new ArrayList<>();
        await().atMost(5, TimeUnit.SECONDS)
                .until(
                        () -> {
                            read.addAll(source.read());
                            return read.size() >= numberOfRecords;
                        });
        return read;
    }

    static byte[] serializeGenericRecord(GenericRecord record) throws IOException {
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());
        // enable Decimal conversion, otherwise attempting to serialize java.math.BigDecimal will
        // throw ClassCastException.
        writer.getData().addLogicalTypeConversion(new Conversions.DecimalConversion());
        ByteArrayOutputStream oo = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(oo, null);
        writer.write(record, encoder);
        return oo.toByteArray();
    }

    static class TestSourceService extends AgentServiceGrpc.AgentServiceImplBase {

        final List<Long> committedRecords = new CopyOnWriteArrayList<>();
        PermanentFailure permanentFailure;

        @Override
        public StreamObserver<SourceRequest> read(StreamObserver<SourceResponse> responseObserver) {

            String schemaStr =
                    "{\"type\":\"record\",\"name\":\"testRecord\",\"fields\":[{\"name\":\"testField\",\"type\":\"string\"}]}";
            org.apache.avro.Schema avroSchema =
                    new org.apache.avro.Schema.Parser().parse(schemaStr);
            GenericData.Record avroRecord = new GenericData.Record(avroSchema);
            avroRecord.put("testField", "test-string");
            try {
                responseObserver.onNext(
                        SourceResponse.newBuilder()
                                .setSchema(
                                        Schema.newBuilder()
                                                .setValue(ByteString.copyFromUtf8(schemaStr))
                                                .setSchemaId(42)
                                                .build())
                                .addRecords(
                                        ai.langstream.agents.grpc.Record.newBuilder()
                                                .setRecordId(42)
                                                .setValue(
                                                        Value.newBuilder()
                                                                .setSchemaId(42)
                                                                .setAvroValue(
                                                                        ByteString.copyFrom(
                                                                                serializeGenericRecord(
                                                                                        avroRecord)))))
                                .build());
                responseObserver.onNext(
                        SourceResponse.newBuilder()
                                .addRecords(
                                        ai.langstream.agents.grpc.Record.newBuilder()
                                                .setRecordId(43))
                                .build());
                responseObserver.onNext(
                        SourceResponse.newBuilder()
                                .addRecords(
                                        ai.langstream.agents.grpc.Record.newBuilder()
                                                .setRecordId(44))
                                .build());
            } catch (IOException e) {
                responseObserver.onError(e);
            }

            return new StreamObserver<>() {
                @Override
                public void onNext(SourceRequest request) {
                    committedRecords.addAll(request.getCommittedRecordsList());
                    if (request.hasPermanentFailure()) {
                        permanentFailure = request.getPermanentFailure();
                    }
                    if (request.getCommittedRecordsList().contains(43L)) {
                        responseObserver.onError(new RuntimeException("test error"));
                    } else if (request.getCommittedRecordsList().contains(44L)) {
                        responseObserver.onCompleted();
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
    }

    static class TestAgentContext implements AgentContext {

        private final CountDownLatch failureCalled = new CountDownLatch(1);

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
            failureCalled.countDown();
        }

        @Override
        public Path getCodeDirectory() {
            return null;
        }
    }
}
