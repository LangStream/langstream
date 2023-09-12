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
package ai.langstream.apigateway.websocket.handlers;

import static ai.langstream.apigateway.websocket.WebSocketConfig.CONSUME_PATH;

import ai.langstream.api.model.Application;
import ai.langstream.api.model.Gateway;
import ai.langstream.api.model.StreamingCluster;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.topics.OffsetPerPartition;
import ai.langstream.api.runner.topics.TopicConnectionsRuntime;
import ai.langstream.api.runner.topics.TopicConnectionsRuntimeRegistry;
import ai.langstream.api.runner.topics.TopicOffsetPosition;
import ai.langstream.api.runner.topics.TopicReadResult;
import ai.langstream.api.runner.topics.TopicReader;
import ai.langstream.api.storage.ApplicationStore;
import ai.langstream.apigateway.websocket.AuthenticatedGatewayRequestContext;
import ai.langstream.apigateway.websocket.api.ConsumePushMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

@Slf4j
public class ConsumeHandler extends AbstractHandler {

    private final ExecutorService executor;

    public ConsumeHandler(
            ApplicationStore applicationStore,
            ExecutorService executor,
            TopicConnectionsRuntimeRegistry topicConnectionsRuntimeRegistry) {
        super(applicationStore, topicConnectionsRuntimeRegistry);
        this.executor = executor;
    }

    @Override
    public String path() {
        return CONSUME_PATH;
    }

    @Override
    Gateway.GatewayType gatewayType() {
        return Gateway.GatewayType.consume;
    }

    @Override
    String tenantFromPath(Map<String, String> parsedPath, Map<String, String> queryString) {
        return parsedPath.get("tenant");
    }

    @Override
    String applicationIdFromPath(Map<String, String> parsedPath, Map<String, String> queryString) {
        return parsedPath.get("application");
    }

    @Override
    String gatewayFromPath(Map<String, String> parsedPath, Map<String, String> queryString) {
        return parsedPath.get("gateway");
    }

    @Override
    public void onBeforeHandshakeCompleted(
            AuthenticatedGatewayRequestContext context, Map<String, Object> attributes)
            throws Exception {
        final Gateway gateway = context.gateway();
        final Application application = context.application();
        List<Function<Record, Boolean>> filters =
                createMessageFilters(gateway, context.userParameters(), context.principalValues());

        context.attributes().put("consumeFilters", filters);

        final StreamingCluster streamingCluster = application.getInstance().streamingCluster();

        final TopicConnectionsRuntime topicConnectionsRuntime =
                topicConnectionsRuntimeRegistry
                        .getTopicConnectionsRuntime(streamingCluster)
                        .asTopicConnectionsRuntime();

        final String topicName = gateway.getTopic();

        final String positionParameter = context.options().getOrDefault("position", "latest");
        TopicOffsetPosition position =
                switch (positionParameter) {
                    case "latest" -> TopicOffsetPosition.LATEST;
                    case "earliest" -> TopicOffsetPosition.EARLIEST;
                    default -> TopicOffsetPosition.absolute(
                            new String(
                                    Base64.getDecoder().decode(positionParameter),
                                    StandardCharsets.UTF_8));
                };
        TopicReader reader =
                topicConnectionsRuntime.createReader(
                        streamingCluster, Map.of("topic", topicName), position);
        reader.start();
        context.attributes().put("topicReader", reader);
        sendClientConnectedEvent(context);
    }

    @Override
    public void onOpen(WebSocketSession session, AuthenticatedGatewayRequestContext context) {
        // we must return the caller thread to the thread pool
        final CompletableFuture<Void> future =
                CompletableFuture.runAsync(
                        () -> {
                            final Map<String, Object> attributes = session.getAttributes();
                            TopicReader reader = (TopicReader) attributes.get("topicReader");
                            final String tenant = context.tenant();
                            final String gatewayId = context.gateway().getId();
                            final String applicationId = context.applicationId();
                            try {
                                log.info(
                                        "[{}] Started reader for gateway {}/{}/{}",
                                        session.getId(),
                                        tenant,
                                        applicationId,
                                        gatewayId);
                                readMessages(
                                        session,
                                        (List<Function<Record, Boolean>>)
                                                attributes.get("consumeFilters"),
                                        reader);
                            } catch (InterruptedException | CancellationException ex) {
                                // ignore
                            } catch (Throwable ex) {
                                log.error(ex.getMessage(), ex);
                            } finally {
                                closeReader(reader);
                            }
                        },
                        executor);
        session.getAttributes().put("future", future);
    }

    @Override
    public void onMessage(
            WebSocketSession webSocketSession,
            AuthenticatedGatewayRequestContext context,
            TextMessage message) {}

    @Override
    public void onClose(
            WebSocketSession webSocketSession,
            AuthenticatedGatewayRequestContext context,
            CloseStatus closeStatus) {
        final CompletableFuture<Void> future =
                (CompletableFuture<Void>) webSocketSession.getAttributes().get("future");
        if (future != null && !future.isDone()) {
            future.cancel(true);
        }
    }

    @Override
    void validateOptions(Map<String, String> options) {
        for (Map.Entry<String, String> option : options.entrySet()) {
            switch (option.getKey()) {
                case "position":
                    if (!StringUtils.hasText(option.getValue())) {
                        throw new IllegalArgumentException("'position' cannot be blank");
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Unknown option " + option.getKey());
            }
        }
    }

    private void closeReader(TopicReader reader) {
        if (reader == null) {
            return;
        }
        try {
            reader.close();
        } catch (Exception e) {
            log.error("error closing reader", e);
        }
    }

    private void readMessages(
            WebSocketSession session, List<Function<Record, Boolean>> filters, TopicReader reader)
            throws Exception {
        while (true) {
            if (Thread.currentThread().isInterrupted()) {
                return;
            }
            if (!session.isOpen()) {
                return;
            }
            final TopicReadResult readResult = reader.read();
            final List<Record> records = readResult.records();
            for (Record record : records) {
                log.debug("[{}] Received record {}", session.getId(), record);
                boolean skip = false;
                for (Function<Record, Boolean> filter : filters) {
                    if (!filter.apply(record)) {
                        skip = true;
                        log.debug("[{}] Skipping record {}", session.getId(), record);
                        break;
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
                    session.sendMessage(new TextMessage(jsonMessage));
                }
            }
        }
    }

    private Map<String, String> computeMessageHeaders(Record record) {
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

    private String computeOffset(TopicReadResult readResult) throws JsonProcessingException {
        final OffsetPerPartition offsetPerPartition = readResult.partitionsOffsets();
        if (offsetPerPartition == null) {
            return null;
        }
        return Base64.getEncoder().encodeToString(mapper.writeValueAsBytes(offsetPerPartition));
    }

    private List<Function<Record, Boolean>> createMessageFilters(
            Gateway selectedGateway,
            Map<String, String> passedParameters,
            Map<String, String> principalValues) {
        List<Function<Record, Boolean>> filters = new ArrayList<>();

        if (selectedGateway.getConsumeOptions() != null) {
            final Gateway.ConsumeOptions consumeOptions = selectedGateway.getConsumeOptions();
            if (consumeOptions.filters() != null) {
                if (consumeOptions.filters().headers() != null) {
                    for (Gateway.KeyValueComparison comparison :
                            consumeOptions.filters().headers()) {
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
                                        value =
                                                passedParameters.get(
                                                        comparison.valueFromParameters());
                                    }
                                    if (value == null
                                            && comparison.valueFromAuthentication() != null) {
                                        value =
                                                principalValues.get(
                                                        comparison.valueFromAuthentication());
                                    }
                                    if (value == null) {
                                        return false;
                                    }
                                    return expectedValue.equals(value);
                                });
                    }
                }
            }
        }
        return filters;
    }
}
