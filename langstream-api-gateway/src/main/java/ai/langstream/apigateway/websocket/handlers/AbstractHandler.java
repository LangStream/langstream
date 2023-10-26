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

import ai.langstream.api.events.EventRecord;
import ai.langstream.api.events.EventSources;
import ai.langstream.api.events.GatewayEventData;
import ai.langstream.api.model.Gateway;
import ai.langstream.api.model.StreamingCluster;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SimpleRecord;
import ai.langstream.api.runner.topics.TopicConnectionsRuntime;
import ai.langstream.api.runner.topics.TopicConnectionsRuntimeRegistry;
import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.api.runner.topics.TopicReadResult;
import ai.langstream.api.runner.topics.TopicReader;
import ai.langstream.api.storage.ApplicationStore;
import ai.langstream.apigateway.gateways.ConsumeGateway;
import ai.langstream.apigateway.gateways.GatewayRequestHandler;
import ai.langstream.apigateway.gateways.ProduceGateway;
import ai.langstream.apigateway.websocket.AuthenticatedGatewayRequestContext;
import ai.langstream.apigateway.websocket.api.ConsumePushMessage;
import ai.langstream.apigateway.websocket.api.ProduceResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

@Slf4j
public abstract class AbstractHandler extends TextWebSocketHandler {
    protected static final ObjectMapper mapper = new ObjectMapper();
    protected static final String ATTRIBUTE_PRODUCE_GATEWAY = "__produce_gateway";
    protected static final String ATTRIBUTE_CONSUME_GATEWAY = "__consume_gateway";
    protected final TopicConnectionsRuntimeRegistry topicConnectionsRuntimeRegistry;
    protected final ApplicationStore applicationStore;

    public AbstractHandler(
            ApplicationStore applicationStore,
            TopicConnectionsRuntimeRegistry topicConnectionsRuntimeRegistry) {
        this.topicConnectionsRuntimeRegistry = topicConnectionsRuntimeRegistry;
        this.applicationStore = applicationStore;
    }

    public abstract String path();

    public abstract Gateway.GatewayType gatewayType();

    public abstract String tenantFromPath(Map<String, String> parsedPath, Map<String, String> queryString);

    public abstract String applicationIdFromPath(
            Map<String, String> parsedPath, Map<String, String> queryString);

    public abstract String gatewayFromPath(
            Map<String, String> parsedPath, Map<String, String> queryString);


    public abstract GatewayRequestHandler.GatewayRequestValidator validator();

    public void onBeforeHandshakeCompleted(
            AuthenticatedGatewayRequestContext gatewayRequestContext,
            Map<String, Object> attributes)
            throws Exception {
    }

    abstract void onOpen(
            WebSocketSession webSocketSession,
            AuthenticatedGatewayRequestContext gatewayRequestContext)
            throws Exception;

    abstract void onMessage(
            WebSocketSession webSocketSession,
            AuthenticatedGatewayRequestContext gatewayRequestContext,
            TextMessage message)
            throws Exception;

    abstract void onClose(
            WebSocketSession webSocketSession,
            AuthenticatedGatewayRequestContext gatewayRequestContext,
            CloseStatus status)
            throws Exception;

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        super.afterConnectionEstablished(session);
        try {
            onOpen(session, getContext(session));
        } catch (Throwable throwable) {
            log.error("[{}] error while opening websocket", session.getId(), throwable);
            closeSession(session, throwable);
        }
    }

    private AuthenticatedGatewayRequestContext getContext(WebSocketSession session) {
        return (AuthenticatedGatewayRequestContext) session.getAttributes().get("context");
    }

    private void closeSession(WebSocketSession session, Throwable throwable) throws IOException {
        CloseStatus status = CloseStatus.SERVER_ERROR;
        if (throwable instanceof IllegalArgumentException) {
            status = CloseStatus.POLICY_VIOLATION;
        }
        try {
            session.close(status.withReason(throwable.getMessage()));
        } finally {
            callHandlerOnClose(session, status);
        }
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message)
            throws Exception {
        try {
            onMessage(session, getContext(session), message);
        } catch (Throwable throwable) {
            log.error("[{}] error while opening websocket", session.getId(), throwable);
            closeSession(session, throwable);
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status)
            throws Exception {
        super.afterConnectionClosed(session, status);
        callHandlerOnClose(session, status);
        sendClientDisconnectedEvent(getContext(session));
    }

    private void callHandlerOnClose(WebSocketSession session, CloseStatus status) {
        try {
            onClose(session, getContext(session), status);
        } catch (Throwable throwable) {
            log.error("[{}] error while closing websocket", session.getId(), throwable);
        }
    }

    @Override
    public boolean supportsPartialMessages() {
        return true;
    }

    protected void sendClientConnectedEvent(AuthenticatedGatewayRequestContext context) {
        sendEvent(EventRecord.Types.ClientConnected, context);
    }

    protected void sendClientDisconnectedEvent(AuthenticatedGatewayRequestContext context) {
        try {
            sendEvent(EventRecord.Types.ClientDisconnected, context);
        } catch (Throwable e) {
            log.error("error while sending client disconnected event", e);
        }
    }

    @SneakyThrows
    protected void sendEvent(EventRecord.Types type, AuthenticatedGatewayRequestContext context) {
        final Gateway gateway = context.gateway();
        if (gateway.getEventsTopic() == null) {
            return;
        }
        final StreamingCluster streamingCluster =
                context.application().getInstance().streamingCluster();
        final TopicConnectionsRuntime topicConnectionsRuntime =
                topicConnectionsRuntimeRegistry
                        .getTopicConnectionsRuntime(streamingCluster)
                        .asTopicConnectionsRuntime();

        topicConnectionsRuntime.init(streamingCluster);

        try (final TopicProducer producer =
                     topicConnectionsRuntime.createProducer(
                             "langstream-events",
                             streamingCluster,
                             Map.of("topic", gateway.getEventsTopic()))) {
            producer.start();

            final EventSources.GatewaySource source =
                    EventSources.GatewaySource.builder()
                            .tenant(context.tenant())
                            .applicationId(context.applicationId())
                            .gateway(gateway)
                            .build();

            final GatewayEventData data =
                    GatewayEventData.builder()
                            .userParameters(context.userParameters())
                            .options(context.options())
                            .httpRequestHeaders(context.httpHeaders())
                            .build();

            final EventRecord event =
                    EventRecord.builder()
                            .category(EventRecord.Categories.Gateway)
                            .type(type.toString())
                            .timestamp(System.currentTimeMillis())
                            .source(mapper.convertValue(source, Map.class))
                            .data(mapper.convertValue(data, Map.class))
                            .build();

            final String recordValue = mapper.writeValueAsString(event);

            final SimpleRecord record = SimpleRecord.builder().value(recordValue).build();
            producer.write(record).get();
            log.info("sent event {}", recordValue);
        }
    }

    protected void startReadingMessages(WebSocketSession webSocketSession, Executor executor) {
        final AuthenticatedGatewayRequestContext context = getContext(webSocketSession);
        final ConsumeGateway consumeGateway = (ConsumeGateway) context.attributes().get(ATTRIBUTE_CONSUME_GATEWAY);
        final CompletableFuture<Void> future = consumeGateway.startReading(
                executor,
                () -> !webSocketSession.isOpen(),
                message -> {
                    try {
                        webSocketSession.sendMessage(new TextMessage(message));
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                });
        webSocketSession.getAttributes().put("future", future);
    }

    protected static List<Function<Record, Boolean>> createMessageFilters(
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

    protected void setupReader(
            String topic,
            List<Function<Record, Boolean>> filters,
            AuthenticatedGatewayRequestContext context)
            throws Exception {
        final ConsumeGateway consumeGateway = new ConsumeGateway(topicConnectionsRuntimeRegistry);
        context.attributes().put(ATTRIBUTE_CONSUME_GATEWAY, consumeGateway);
        consumeGateway.setup(topic, filters, context);

    }

    protected void stopReadingMessages(WebSocketSession webSocketSession) {
        final CompletableFuture<Void> future =
                (CompletableFuture<Void>) webSocketSession.getAttributes().get("future");
        if (future != null && !future.isDone()) {
            future.cancel(true);
        }
    }


    protected void setupProducer(String topic, List<Header> commonHeaders, AuthenticatedGatewayRequestContext context) {
        final ProduceGateway produceGateway = new ProduceGateway(topicConnectionsRuntimeRegistry);
        context.attributes().put(ATTRIBUTE_PRODUCE_GATEWAY, produceGateway);
        produceGateway.start(topic, commonHeaders, context);
    }


    protected void produceMessage(WebSocketSession webSocketSession,
                                  TextMessage message)
            throws IOException {
        try {
            final AuthenticatedGatewayRequestContext context = getContext(webSocketSession);
            final ProduceGateway produceGateway = (ProduceGateway) context.attributes().get(ATTRIBUTE_PRODUCE_GATEWAY);
            produceGateway.produceMessage(message.getPayload());
            webSocketSession.sendMessage(
                    new TextMessage(mapper.writeValueAsString(ProduceResponse.OK)));
        } catch (ProduceGateway.ProduceException exception) {
            sendResponse(webSocketSession, exception.getStatus(), exception.getMessage());
        }
    }

    protected void closeProduceGateway(WebSocketSession webSocketSession) {
        final ProduceGateway produceGateway = (ProduceGateway) getContext(webSocketSession).attributes().get(
                ATTRIBUTE_PRODUCE_GATEWAY);
        if (produceGateway == null) {
            return;
        }
        produceGateway.close();
    }


    private static void sendResponse(
            WebSocketSession webSocketSession, ProduceResponse.Status status, String reason)
            throws IOException {
        webSocketSession.sendMessage(
                new TextMessage(mapper.writeValueAsString(new ProduceResponse(status, reason))));
    }

}
