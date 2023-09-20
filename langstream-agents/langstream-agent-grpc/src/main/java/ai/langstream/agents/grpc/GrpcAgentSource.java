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

import ai.langstream.api.runner.code.AgentSource;
import ai.langstream.api.runner.code.Record;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GrpcAgentSource extends AbstractGrpcAgent implements AgentSource {

    private static final int MAX_RECORDS_PER_READ = 10_000;
    private StreamObserver<SourceRequest> request;
    private final StreamObserver<SourceResponse> responseObserver;

    // TODO: use a bounded queue ? backpressure ?
    private final ConcurrentLinkedQueue<Record> readRecords = new ConcurrentLinkedQueue<>();

    public GrpcAgentSource() {
        super();
        this.responseObserver = getResponseObserver();
    }

    public GrpcAgentSource(ManagedChannel channel) {
        super(channel);
        this.responseObserver = getResponseObserver();
    }

    @Override
    public void onNewSchemaToSend(Schema schema) {
        throw new UnsupportedOperationException("Shouldn't be called on a source");
    }

    @Override
    public void start() throws Exception {
        super.start();
        request = AgentServiceGrpc.newStub(channel).withWaitForReady().read(responseObserver);
    }

    @Override
    public List<Record> read() throws Exception {
        List<Record> read = new ArrayList<>();
        for (int i = 0; i < MAX_RECORDS_PER_READ; i++) {
            Record record = readRecords.poll();
            if (record == null) {
                break;
            }
            read.add(record);
        }
        return read;
    }

    @Override
    public void commit(List<Record> records) throws Exception {
        SourceRequest.Builder requestBuilder = SourceRequest.newBuilder();
        for (Record record : records) {
            if (record instanceof GrpcAgentRecord grpcAgentRecord) {
                requestBuilder.addCommittedRecords(grpcAgentRecord.id());
            } else {
                throw new IllegalArgumentException(
                        "Record %s is not a GrpcAgentRecord".formatted(record));
            }
        }
        request.onNext(requestBuilder.build());
    }

    private StreamObserver<SourceResponse> getResponseObserver() {
        return new StreamObserver<>() {
            @Override
            public void onNext(SourceResponse response) {
                if (response.hasSchema()) {
                    org.apache.avro.Schema schema =
                            new org.apache.avro.Schema.Parser()
                                    .parse(response.getSchema().getValue().toStringUtf8());
                    serverSchemas.put(response.getSchema().getSchemaId(), schema);
                }
                try {
                    for (ai.langstream.agents.grpc.Record record : response.getRecordsList()) {
                        readRecords.add(fromGrpc(record));
                    }
                } catch (Exception e) {
                    agentContext.criticalFailure(
                            new RuntimeException("Error while processing records", e));
                }
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
