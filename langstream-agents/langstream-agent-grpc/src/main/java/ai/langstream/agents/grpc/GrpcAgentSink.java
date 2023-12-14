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
import ai.langstream.api.util.ConfigurationUtils;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GrpcAgentSink extends AbstractGrpcAgent implements AgentSink {
    private volatile StreamObserver<SinkRequest> request;
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
        request = asyncStub.write(responseObserver);
        restarting.set(false);
        startFailedButDevelopmentMode = false;
    }

    @Override
    public CompletableFuture<?> write(Record record) {
        CompletableFuture<?> handle = new CompletableFuture<>();
        Long rId = recordId.incrementAndGet();
        try {
            SinkRequest.Builder requestBuilder = SinkRequest.newBuilder();
            requestBuilder.setRecord(toGrpc(record).setRecordId(rId));
            writeHandles.put(rId, handle);
            try {
                request.onNext(requestBuilder.build());
            } catch (IllegalStateException stopped) {
                if (restarting.get()) {
                    if (ConfigurationUtils.isDevelopmentMode()) {
                        log.info(
                                "Ignoring error during restart in dev mode {}, "
                                        + "ignoring record {}",
                                stopped + "",
                                record);
                        writeHandles.remove(rId);
                        handle.complete(null);
                    } else {
                        throw stopped;
                    }
                }
            }
        } catch (Throwable error) {
            writeHandles.remove(rId);
            handle.completeExceptionally(error);
        }
        return handle;
    }

    private StreamObserver<SinkResponse> getResponseObserver() {
        return new StreamObserver<>() {
            @Override
            public void onNext(SinkResponse response) {
                if (!writeHandles.containsKey(response.getRecordId())) {
                    agentContext.criticalFailure(
                            new RuntimeException(
                                    "GrpcAgentSink received unknown record id: %s"
                                            .formatted(response.getRecordId())));
                    return;
                }
                CompletableFuture<?> handle = writeHandles.get(response.getRecordId());
                if (response.hasError()) {
                    handle.completeExceptionally(new RuntimeException(response.getError()));
                } else {
                    handle.complete(null);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                if (startFailedButDevelopmentMode || restarting.get()) {
                    log.info(
                            "Ignoring error during restart in dev mode {}, "
                                    + "ignoring records {}",
                            throwable + "",
                            writeHandles);
                    writeHandles.forEach((id, handle) -> handle.complete(null));
                    writeHandles.clear();
                } else {
                    agentContext.criticalFailure(
                            new RuntimeException(
                                    "gRPC server sent error: %s".formatted(throwable.getMessage()),
                                    throwable));
                }
            }

            @Override
            public void onCompleted() {
                if (startFailedButDevelopmentMode || restarting.get()) {
                    log.info(
                            "Ignoring server completion during restart in dev mode, "
                                    + "ignoring records {}",
                            writeHandles);
                    writeHandles.forEach((id, handle) -> handle.complete(null));
                    writeHandles.clear();
                } else {
                    agentContext.criticalFailure(
                            new RuntimeException("gRPC server completed the stream unexpectedly"));
                }
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
