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

import ai.langstream.api.runner.code.AgentSink;
import ai.langstream.api.runner.code.Record;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GrpcAgentSink extends AbstractGrpcAgent implements AgentSink {
    private StreamObserver<SinkRequest> request;
    private final StreamObserver<SinkResponse> responseObserver;

    // For each record sent, we increment the recordId
    protected final AtomicLong recordId = new AtomicLong(0);
    private final Map<Long, CompletableFuture<?>> writeHandles = new ConcurrentHashMap<>();

    public GrpcAgentSink() {
        super();
        this.responseObserver = getResponseObserver();
    }

    public GrpcAgentSink(ManagedChannel channel) {
        super(channel);
        this.responseObserver = getResponseObserver();
    }

    @Override
    public void onNewSchemaToSend(Schema schema) {
        request.onNext(SinkRequest.newBuilder().setSchema(schema).build());
    }

    @Override
    public void start() throws Exception {
        super.start();
        request = AgentServiceGrpc.newStub(channel).withWaitForReady().write(responseObserver);
    }

    @Override
    public CompletableFuture<?> write(Record record) {
        CompletableFuture<?> handle = new CompletableFuture<>();
        long rId = recordId.incrementAndGet();
        SinkRequest.Builder requestBuilder = SinkRequest.newBuilder();
        try {
            requestBuilder.setRecord(toGrpc(record).setRecordId(rId));
        } catch (IOException e) {
            agentContext.criticalFailure(new RuntimeException("Error while processing records", e));
        }
        writeHandles.put(rId, handle);
        request.onNext(requestBuilder.build());
        return handle;
    }

    private StreamObserver<SinkResponse> getResponseObserver() {
        return new StreamObserver<>() {
            @Override
            public void onNext(SinkResponse response) {
                CompletableFuture<?> handle = writeHandles.get(response.getRecordId());
                if (response.hasError()) {
                    handle.completeExceptionally(new RuntimeException(response.getError()));
                } else {
                    handle.complete(null);
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
