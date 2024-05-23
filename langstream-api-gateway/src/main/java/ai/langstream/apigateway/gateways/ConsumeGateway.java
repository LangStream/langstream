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
package ai.langstream.apigateway.gateways;

import ai.langstream.api.model.Gateway;
import ai.langstream.api.model.StreamingCluster;
import ai.langstream.api.model.TopicDefinition;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.topics.TopicConnectionsRuntime;
import ai.langstream.api.runner.topics.TopicConnectionsRuntimeRegistry;
import ai.langstream.api.runner.topics.TopicOffsetPosition;
import ai.langstream.api.runner.topics.TopicReadResult;
import ai.langstream.api.runner.topics.TopicReader;
import ai.langstream.api.runtime.ClusterRuntimeRegistry;
import ai.langstream.api.runtime.StreamingClusterRuntime;
import ai.langstream.api.runtime.Topic;
import ai.langstream.apigateway.api.ConsumePushMessage;
import ai.langstream.apigateway.websocket.AuthenticatedGatewayRequestContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConsumeGateway implements AutoCloseable {

    protected static final ObjectMapper mapper = new ObjectMapper();
    private final TopicConnectionsRuntimeRegistry topicConnectionsRuntimeRegistry;
    private final ClusterRuntimeRegistry clusterRuntimeRegistry;

    private volatile TopicConnectionsRuntime topicConnectionsRuntime;

    private volatile TopicReader reader;
    private volatile boolean interrupted;
    private volatile String logRef;
    private CompletableFuture<Void> readerFuture;
    private AuthenticatedGatewayRequestContext requestContext;
    private List<Function<Record, Boolean>> filters;

    public ConsumeGateway(
            TopicConnectionsRuntimeRegistry topicConnectionsRuntimeRegistry,
            ClusterRuntimeRegistry clusterRuntimeRegistry) {
        this.topicConnectionsRuntimeRegistry = topicConnectionsRuntimeRegistry;
        this.clusterRuntimeRegistry = clusterRuntimeRegistry;
    }

    public void setup(
            String topic,
            List<Function<Record, Boolean>> filters,
            AuthenticatedGatewayRequestContext requestContext)
            throws Exception {
        this.logRef =
                "%s/%s/%s"
                        .formatted(
                                requestContext.tenant(),
                                requestContext.applicationId(),
                                requestContext.gateway().getId());
        this.requestContext = requestContext;
        this.filters = filters == null ? List.of() : filters;

        final StreamingCluster streamingCluster =
                requestContext.application().getInstance().streamingCluster();
        topicConnectionsRuntime =
                topicConnectionsRuntimeRegistry
                        .getTopicConnectionsRuntime(streamingCluster)
                        .asTopicConnectionsRuntime();

        topicConnectionsRuntime.init(streamingCluster);

        final String positionParameter =
                requestContext.options().getOrDefault("position", "latest");
        TopicOffsetPosition position =
                switch (positionParameter) {
                    case "latest" -> TopicOffsetPosition.LATEST;
                    case "earliest" -> TopicOffsetPosition.EARLIEST;
                    default -> TopicOffsetPosition.absolute(
                            Base64.getDecoder().decode(positionParameter));
                };
        TopicDefinition topicDefinition = requestContext.application().resolveTopic(topic);
        StreamingClusterRuntime streamingClusterRuntime =
                clusterRuntimeRegistry.getStreamingClusterRuntime(streamingCluster);
        Topic topicImplementation =
                streamingClusterRuntime.createTopicImplementation(
                        topicDefinition, streamingCluster);
        final String resolvedTopicName = topicImplementation.topicName();
        reader =
                topicConnectionsRuntime.createReader(
                        streamingCluster, Map.of("topic", resolvedTopicName), position);
        reader.start();
    }

    public void startReadingAsync(
            Executor executor, Supplier<Boolean> stop, Consumer<String> onMessage) {
        if (requestContext == null || reader == null) {
            throw new IllegalStateException("Not initialized");
        }
        if (readerFuture != null) {
            throw new IllegalStateException("Already started");
        }
        readerFuture =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                log.debug("[{}] Started reader", logRef);
                                readMessages(stop, onMessage);
                            } catch (Throwable ex) {
                                log.error("[{}] Error reading messages", logRef, ex);
                                throw new RuntimeException(ex);
                            } finally {
                                closeReader();
                            }
                        },
                        executor);
    }

    private void readMessages(Supplier<Boolean> stop, Consumer<String> onMessage) throws Exception {
        while (true) {
            if (interrupted) {
                return;
            }
            if (stop.get()) {
                return;
            }
            final TopicReadResult readResult = reader.read();
            final List<Record> records = readResult.records();
            for (Record record : records) {
                log.debug("[{}] Received record {}", logRef, record);
                boolean skip = false;
                if (filters != null) {
                    for (Function<Record, Boolean> filter : filters) {
                        if (!filter.apply(record)) {
                            skip = true;
                            log.debug("[{}] Skipping record {}", logRef, record);
                            break;
                        }
                    }
                }
                if (!skip) {
                    final Map<String, String> messageHeaders = computeMessageHeaders(record);
                    final String offset = computeOffset(readResult);

                    final ConsumePushMessage message =
                            new ConsumePushMessage(
                                    new ConsumePushMessage.Record(
                                            record.key(), record.value(), messageHeaders),
                                    offset);
                    final String jsonMessage = mapper.writeValueAsString(message);
                    onMessage.accept(jsonMessage);
                }
            }
        }
    }

    private static Map<String, String> computeMessageHeaders(Record record) {
        final Collection<Header> headers = record.headers();
        final Map<String, String> messageHeaders;
        if (headers == null) {
            messageHeaders = Map.of();
        } else {
            messageHeaders = new HashMap<>();
            headers.forEach(h -> messageHeaders.put(h.key(), h.valueAsString()));
        }
        return messageHeaders;
    }

    private static String computeOffset(TopicReadResult readResult) {
        final byte[] offset = readResult.offset();
        if (offset == null) {
            return null;
        }
        return Base64.getEncoder().encodeToString(offset);
    }

    private void closeReader() {
        if (reader != null) {
            try {
                reader.close();
            } catch (Exception e) {
                log.warn("error closing reader", e);
            }
        }
        if (topicConnectionsRuntime != null) {
            try {
                topicConnectionsRuntime.close();
            } catch (Exception e) {
                log.warn("error closing runtime", e);
            }
        }
    }

    @Override
    public void close() {
        if (readerFuture != null) {

            interrupted = true;
            try {
                // reader.close must be done by the same thread that started the consumer
                readerFuture.get(10, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                log.debug("error waiting for reader to stop", e);
            }
        } else {
            closeReader();
        }
    }

    public static List<Function<Record, Boolean>> createMessageFilters(
            List<Gateway.KeyValueComparison> headersFilters,
            Map<String, String> passedParameters,
            Map<String, String> principalValues) {
        List<Function<Record, Boolean>> filters = new ArrayList<>();
        if (headersFilters == null) {
            return filters;
        }
        for (Gateway.KeyValueComparison comparison : headersFilters) {
            if (comparison.key() == null) {
                throw new IllegalArgumentException("Key cannot be null");
            }
            filters.add(
                    record -> {
                        final Header header = record.getHeader(comparison.key());
                        if (header == null) {
                            return false;
                        }
                        final String expectedValue = header.valueAsString();
                        if (expectedValue == null) {
                            return false;
                        }
                        String value = comparison.value();
                        if (value == null && comparison.valueFromParameters() != null) {
                            value = passedParameters.get(comparison.valueFromParameters());
                        }
                        if (value == null && comparison.valueFromAuthentication() != null) {
                            value = principalValues.get(comparison.valueFromAuthentication());
                        }
                        if (value == null) {
                            return false;
                        }
                        return expectedValue.equals(value);
                    });
        }
        return filters;
    }
}
