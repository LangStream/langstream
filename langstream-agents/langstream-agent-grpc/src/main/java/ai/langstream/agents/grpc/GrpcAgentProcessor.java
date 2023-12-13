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

import ai.langstream.api.runner.code.AgentProcessor;
import ai.langstream.api.runner.code.RecordSink;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GrpcAgentProcessor extends AbstractGrpcAgent implements AgentProcessor {
    private volatile StreamObserver<ProcessorRequest> request;
    private RecordSink sink;

    // For each record sent, we increment the recordId
    protected final AtomicLong recordId = new AtomicLong(0);

    // For each record sent, we store the record and the sink to which the result should be emitted
    private final Map<Long, RecordAndSink> sourceRecords = new ConcurrentHashMap<>();

    private final StreamObserver<ProcessorResponse> responseObserver = getResponseObserver();

    private record RecordAndSink(
            ai.langstream.api.runner.code.Record sourceRecord, RecordSink sink) {}

    public GrpcAgentProcessor() {
        super();
    }

    public GrpcAgentProcessor(ManagedChannel channel) {
        super(channel);
    }

    @Override
    public synchronized void onNewSchemaToSend(Schema schema) {
        request.onNext(ProcessorRequest.newBuilder().setSchema(schema).build());
    }

    @Override
    public void start() throws Exception {
        super.start();
        request = asyncStub.process(responseObserver);
        restarting.set(false);
        startFailedButDevelopmentMode = false;
    }

    @Override
    public void process(List<ai.langstream.api.runner.code.Record> records, RecordSink recordSink) {

        if (startFailedButDevelopmentMode) {
            log.info(
                    "Python agent start failed but development mode is enabled, ignoring {} records",
                    records.size());
            records.forEach(recordSink::emitEmptyList);
            return;
        }

        synchronized (this) {
            if (sink == null) {
                sink = recordSink;
            }
            ProcessorRequest.Builder requestBuilder = ProcessorRequest.newBuilder();
            for (ai.langstream.api.runner.code.Record record : records) {
                long rId = recordId.incrementAndGet();
                try {
                    requestBuilder.addRecords(toGrpc(record).setRecordId(rId));
                    sourceRecords.put(rId, new RecordAndSink(record, recordSink));
                } catch (Exception e) {
                    recordSink.emit(new SourceRecordAndResult(record, null, e));
                }
            }
            if (requestBuilder.getRecordsCount() > 0) {
                try {
                    request.onNext(requestBuilder.build());
                } catch (IllegalStateException stopped) {
                    if (restarting.get()) {
                        log.info(
                                "Ignoring error during restart {}, ignoring results for {} records",
                                stopped + "",
                                records.size());
                        records.forEach(recordSink::emitEmptyList);
                    } else {
                        records.forEach(e -> recordSink.emitError(e, stopped));
                    }
                }
            }
        }
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

    private StreamObserver<ProcessorResponse> getResponseObserver() {
        return new StreamObserver<>() {
            @Override
            public void onNext(ProcessorResponse response) {
                try {
                    if (response.hasSchema()) {
                        org.apache.avro.Schema schema =
                                new org.apache.avro.Schema.Parser()
                                        .parse(response.getSchema().getValue().toStringUtf8());
                        serverSchemas.put(response.getSchema().getSchemaId(), schema);
                    }
                    for (ProcessorResult result : response.getResultsList()) {
                        RecordAndSink recordAndSink = sourceRecords.remove(result.getRecordId());
                        if (recordAndSink == null) {
                            throw new IllegalArgumentException(
                                    "Received unknown record id " + result.getRecordId());
                        } else {
                            recordAndSink
                                    .sink()
                                    .emit(fromGrpc(recordAndSink.sourceRecord(), result));
                        }
                    }
                } catch (Exception e) {
                    agentContext.criticalFailure(
                            new RuntimeException(
                                    "GrpcAgentProcessor error while processing record: %s"
                                            .formatted(e.getMessage()),
                                    e));
                }
            }

            @Override
            public void onError(Throwable throwable) {
                if (!restarting.get()) {
                    agentContext.criticalFailure(
                            new RuntimeException(
                                    "gRPC server sent error: %s".formatted(throwable.getMessage()),
                                    throwable));
                } else {
                    log.info("Ignoring error during restart {}", throwable + "");
                }
            }

            @Override
            public void onCompleted() {
                if (!restarting.get()) {
                    agentContext.criticalFailure(
                            new RuntimeException("gRPC server completed the stream unexpectedly"));
                } else {
                    log.info("Ignoring error server stop during restart");
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
