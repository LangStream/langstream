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
import ai.langstream.api.util.ConfigurationUtils;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GrpcAgentSource extends AbstractGrpcAgent implements AgentSource {

    private static final int MAX_RECORDS_PER_READ = 1000;
    public static final int READ_BUFER_SIZE = 1000;
    private volatile StreamObserver<SourceRequest> request;
    private final StreamObserver<SourceResponse> responseObserver;
    private final LinkedBlockingQueue<Record> readRecords =
            new LinkedBlockingQueue<>(READ_BUFER_SIZE);

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
        request = asyncStub.read(responseObserver);
        restarting.set(false);
        startFailedButDevelopmentMode = false;
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
    public void permanentFailure(Record record, Exception error) {
        if (record instanceof GrpcAgentRecord grpcAgentRecord) {
            request.onNext(
                    SourceRequest.newBuilder()
                            .setPermanentFailure(
                                    PermanentFailure.newBuilder()
                                            .setRecordId(grpcAgentRecord.id())
                                            .setErrorMessage(error.getMessage()))
                            .build());
        } else {
            throw new IllegalArgumentException(
                    "Record %s is not a GrpcAgentRecord".formatted(record));
        }
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
                try {
                    if (response.hasSchema()) {
                        org.apache.avro.Schema schema =
                                new org.apache.avro.Schema.Parser()
                                        .parse(response.getSchema().getValue().toStringUtf8());
                        serverSchemas.put(response.getSchema().getSchemaId(), schema);
                    }

                    for (ai.langstream.agents.grpc.Record record : response.getRecordsList()) {
                        readRecords.add(fromGrpc(record));
                    }
                } catch (Exception e) {
                    agentContext.criticalFailure(
                            new RuntimeException("GrpcAgentSource error while reading records", e));
                }
            }

            @Override
            public void onError(Throwable throwable) {
                if (startFailedButDevelopmentMode || restarting.get()) {
                    log.info("Ignoring error while restarting: {}", throwable.getMessage());
                    return;
                }
                if (ConfigurationUtils.isDevelopmentMode()) {
                    log.info("Ignoring error in developer mode: {}", throwable.getMessage());
                    return;
                }
                agentContext.criticalFailure(
                        new RuntimeException(
                                "gRPC server sent error: %s".formatted(throwable.getMessage()),
                                throwable));
            }

            @Override
            public void onCompleted() {
                if (startFailedButDevelopmentMode || restarting.get()) {
                    log.info("Ignoring completion while restarting");
                    return;
                }
                agentContext.criticalFailure(
                        new RuntimeException("gRPC server completed the stream unexpectedly"));
            }
        };
    }

    protected void stopBeforeRestart() throws Exception {
        log.info("Stopping...");
        restarting.set(true);
        synchronized (this) {
            if (request != null) {
                try {
                    request.onCompleted();
                } catch (IllegalStateException e) {
                    log.info("Ignoring error while stopping {}", e + "");
                }
            }
        }
        super.stopBeforeRestart();
        log.info("Stopped");
    }
}
