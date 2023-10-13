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
import ai.langstream.api.gateway.GatewayRequestContext;
import ai.langstream.api.model.Application;
import ai.langstream.api.model.ApplicationSpecs;
import ai.langstream.api.model.Gateway;
import ai.langstream.api.model.Gateways;
import ai.langstream.api.model.StreamingCluster;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SimpleRecord;
import ai.langstream.api.runner.topics.TopicConnectionsRuntime;
import ai.langstream.api.runner.topics.TopicConnectionsRuntimeRegistry;
import ai.langstream.api.runner.topics.TopicOffsetPosition;
import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.api.runner.topics.TopicReadResult;
import ai.langstream.api.runner.topics.TopicReader;
import ai.langstream.api.storage.ApplicationStore;
import ai.langstream.apigateway.websocket.AuthenticatedGatewayRequestContext;
import ai.langstream.apigateway.websocket.api.ConsumePushMessage;
import ai.langstream.apigateway.websocket.api.ProduceRequest;
import ai.langstream.apigateway.websocket.api.ProduceResponse;
import ai.langstream.apigateway.websocket.impl.GatewayRequestContextImpl;
import ai.langstream.impl.common.ApplicationPlaceholderResolver;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

@Slf4j
public abstract class AbstractHandler extends TextWebSocketHandler {
    protected static final ObjectMapper mapper = new ObjectMapper();
    protected final TopicConnectionsRuntimeRegistry topicConnectionsRuntimeRegistry;
    protected final ApplicationStore applicationStore;

    public AbstractHandler(
            ApplicationStore applicationStore,
            TopicConnectionsRuntimeRegistry topicConnectionsRuntimeRegistry) {
        this.topicConnectionsRuntimeRegistry = topicConnectionsRuntimeRegistry;
        this.applicationStore = applicationStore;
    }

    public abstract String path();

    abstract Gateway.GatewayType gatewayType();

    abstract String tenantFromPath(Map<String, String> parsedPath, Map<String, String> queryString);

    abstract String applicationIdFromPath(
            Map<String, String> parsedPath, Map<String, String> queryString);

    abstract String gatewayFromPath(
            Map<String, String> parsedPath, Map<String, String> queryString);

    public void onBeforeHandshakeCompleted(
            AuthenticatedGatewayRequestContext gatewayRequestContext,
            Map<String, Object> attributes)
            throws Exception {}

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

    abstract void validateOptions(Map<String, String> options);

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
            closeCloseableResources(session);
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
        try {
            onClose(session, getContext(session), status);
        } catch (Throwable throwable) {
            log.error("[{}] error while closing websocket", session.getId(), throwable);
        }
        closeCloseableResources(session);
        sendClientDisconnectedEvent(getContext(session));
    }

    @Override
    public boolean supportsPartialMessages() {
        return true;
    }

    private Application getResolvedApplication(String tenant, String applicationId) {
        final ApplicationSpecs applicationSpecs = applicationStore.getSpecs(tenant, applicationId);
        if (applicationSpecs == null) {
            throw new IllegalArgumentException("application " + applicationId + " not found");
        }
        final Application application = applicationSpecs.getApplication();
        application.setSecrets(applicationStore.getSecrets(tenant, applicationId));
        return ApplicationPlaceholderResolver.resolvePlaceholders(application);
    }

    private Gateway extractGateway(
            String gatewayId, Application application, Gateway.GatewayType type) {
        final Gateways gatewaysObj = application.getGateways();
        if (gatewaysObj == null) {
            throw new IllegalArgumentException("no gateways defined for the application");
        }
        final List<Gateway> gateways = gatewaysObj.gateways();
        if (gateways == null) {
            throw new IllegalArgumentException("no gateways defined for the application");
        }

        Gateway selectedGateway = null;

        for (Gateway gateway : gateways) {
            if (gateway.getId().equals(gatewayId) && type == gateway.getType()) {
                selectedGateway = gateway;
                break;
            }
        }
        if (selectedGateway == null) {
            throw new IllegalArgumentException(
                    "gateway "
                            + gatewayId
                            + " of type "
                            + type
                            + " is not defined in the application");
        }
        return selectedGateway;
    }

    public GatewayRequestContext validateRequest(
            Map<String, String> pathVars,
            Map<String, String> queryString,
            Map<String, String> httpHeaders) {
        Map<String, String> options = new HashMap<>();
        Map<String, String> userParameters = new HashMap<>();

        final String credentials = queryString.remove("credentials");
        final String testCredentials = queryString.remove("test-credentials");

        for (Map.Entry<String, String> entry : queryString.entrySet()) {
            if (entry.getKey().startsWith("option:")) {
                options.put(entry.getKey().substring("option:".length()), entry.getValue());
            } else if (entry.getKey().startsWith("param:")) {
                userParameters.put(entry.getKey().substring("param:".length()), entry.getValue());
            } else {
                throw new IllegalArgumentException(
                        "invalid query parameter "
                                + entry.getKey()
                                + ". "
                                + "To specify a gateway parameter, use the format param:<parameter_name>."
                                + "To specify a option, use the format option:<option_name>.");
            }
        }

        final String tenant = tenantFromPath(pathVars, queryString);
        final String applicationId = applicationIdFromPath(pathVars, queryString);
        final String gatewayId = gatewayFromPath(pathVars, queryString);

        final Application application = getResolvedApplication(tenant, applicationId);
        final Gateway.GatewayType type = gatewayType();
        final Gateway gateway = extractGateway(gatewayId, application, type);

        final List<String> requiredParameters = getAllRequiredParameters(gateway);
        Set<String> allUserParameterKeys = new HashSet<>(userParameters.keySet());
        if (requiredParameters != null) {
            for (String requiredParameter : requiredParameters) {
                final String value = userParameters.get(requiredParameter);
                if (!StringUtils.hasText(value)) {
                    throw new IllegalArgumentException(
                            formatErrorMessage(
                                    tenant,
                                    applicationId,
                                    gateway,
                                    "missing required parameter "
                                            + requiredParameter
                                            + ". Required parameters: "
                                            + requiredParameters));
                }
                allUserParameterKeys.remove(requiredParameter);
            }
        }
        if (!allUserParameterKeys.isEmpty()) {
            throw new IllegalArgumentException(
                    formatErrorMessage(
                            tenant,
                            applicationId,
                            gateway,
                            "unknown parameters: " + allUserParameterKeys));
        }
        validateOptions(options);

        if (credentials != null && testCredentials != null) {
            throw new IllegalArgumentException(
                    formatErrorMessage(
                            tenant,
                            applicationId,
                            gateway,
                            "credentials and test-credentials cannot be used together"));
        }
        return GatewayRequestContextImpl.builder()
                .tenant(tenant)
                .applicationId(applicationId)
                .application(application)
                .credentials(credentials)
                .testCredentials(testCredentials)
                .httpHeaders(httpHeaders)
                .options(options)
                .userParameters(userParameters)
                .gateway(gateway)
                .build();
    }

    private static String formatErrorMessage(
            String tenant, String applicationId, Gateway gateway, String error) {
        return "Error for gateway %s (tenant: %s, appId: %s): %s"
                .formatted(gateway.getId(), tenant, applicationId, error);
    }

    protected abstract List<String> getAllRequiredParameters(Gateway gateway);

    protected static void recordCloseableResource(
            Map<String, Object> attributes, AutoCloseable... closeables) {
        List<AutoCloseable> currentCloseable = (List<AutoCloseable>) attributes.get("closeables");

        if (currentCloseable == null) {
            currentCloseable = new ArrayList<>();
        }
        Collections.addAll(currentCloseable, closeables);
        attributes.put("closeables", currentCloseable);
    }

    private void closeCloseableResources(WebSocketSession webSocketSession) {
        List<AutoCloseable> currentCloseable =
                (List<AutoCloseable>) webSocketSession.getAttributes().get("closeables");

        if (currentCloseable != null) {
            for (AutoCloseable autoCloseable : currentCloseable) {
                try {
                    autoCloseable.close();
                } catch (Throwable e) {
                    log.error("error while closing resource", e);
                }
            }
        }
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

    protected static void startReadingMessages(
            WebSocketSession session,
            AuthenticatedGatewayRequestContext context,
            Executor executor) {
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

    protected static void readMessages(
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
                if (filters != null) {
                    for (Function<Record, Boolean> filter : filters) {
                        if (!filter.apply(record)) {
                            skip = true;
                            log.debug("[{}] Skipping record {}", session.getId(), record);
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
                    session.sendMessage(new TextMessage(jsonMessage));
                }
            }
        }
    }

    private static void closeReader(TopicReader reader) {
        if (reader == null) {
            return;
        }
        try {
            reader.close();
        } catch (Exception e) {
            log.error("error closing reader", e);
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
            Map<String, Object> sessionAttributes,
            String topic,
            StreamingCluster streamingCluster,
            List<Function<Record, Boolean>> filters,
            Map<String, String> options)
            throws Exception {
        sessionAttributes.put("consumeFilters", filters);

        final TopicConnectionsRuntime topicConnectionsRuntime =
                topicConnectionsRuntimeRegistry
                        .getTopicConnectionsRuntime(streamingCluster)
                        .asTopicConnectionsRuntime();

        topicConnectionsRuntime.init(streamingCluster);

        final String positionParameter = options.getOrDefault("position", "latest");
        TopicOffsetPosition position =
                switch (positionParameter) {
                    case "latest" -> TopicOffsetPosition.LATEST;
                    case "earliest" -> TopicOffsetPosition.EARLIEST;
                    default -> TopicOffsetPosition.absolute(
                            Base64.getDecoder().decode(positionParameter));
                };
        TopicReader reader =
                topicConnectionsRuntime.createReader(
                        streamingCluster, Map.of("topic", topic), position);
        reader.start();
        sessionAttributes.put("topicReader", reader);
    }

    protected void stopReadingMessages(WebSocketSession webSocketSession) {
        final CompletableFuture<Void> future =
                (CompletableFuture<Void>) webSocketSession.getAttributes().get("future");
        if (future != null && !future.isDone()) {
            future.cancel(true);
        }
    }

    protected void setupProducer(
            Map<String, Object> sessionAttributes,
            String topic,
            StreamingCluster streamingCluster,
            final List<Header> commonHeaders,
            final String tenant,
            final String applicationId,
            final String gatewayId) {
        final TopicConnectionsRuntime topicConnectionsRuntime =
                topicConnectionsRuntimeRegistry
                        .getTopicConnectionsRuntime(streamingCluster)
                        .asTopicConnectionsRuntime();

        topicConnectionsRuntime.init(streamingCluster);

        final TopicProducer producer =
                topicConnectionsRuntime.createProducer(
                        null, streamingCluster, Map.of("topic", topic));
        recordCloseableResource(sessionAttributes, producer);
        producer.start();

        sessionAttributes.put("producer", producer);
        sessionAttributes.put(
                "headers",
                commonHeaders == null ? List.of() : Collections.unmodifiableList(commonHeaders));
        log.info(
                "Started produced for gateway {}/{}/{} on topic {}",
                tenant,
                applicationId,
                gatewayId,
                topic);
    }

    protected static List<Header> getProducerCommonHeaders(
            List<Gateway.KeyValueComparison> headerFilters,
            Map<String, String> passedParameters,
            Map<String, String> principalValues) {
        final List<Header> headers = new ArrayList<>();
        if (headerFilters == null) {
            return headers;
        }
        for (Gateway.KeyValueComparison mapping : headerFilters) {
            if (mapping.key() == null || mapping.key().isEmpty()) {
                throw new IllegalArgumentException("Header key cannot be empty");
            }
            String value = mapping.value();
            if (value == null && mapping.valueFromParameters() != null) {
                value = passedParameters.get(mapping.valueFromParameters());
            }
            if (value == null && mapping.valueFromAuthentication() != null) {
                value = principalValues.get(mapping.valueFromAuthentication());
            }
            if (value == null) {
                throw new IllegalArgumentException("header " + mapping.key() + " cannot be empty");
            }
            headers.add(SimpleRecord.SimpleHeader.of(mapping.key(), value));
        }
        return headers;
    }

    protected static void produceMessage(WebSocketSession webSocketSession, TextMessage message)
            throws IOException {
        final TopicProducer topicProducer = getTopicProducer(webSocketSession, true);
        final ProduceRequest produceRequest;
        try {
            produceRequest = mapper.readValue(message.getPayload(), ProduceRequest.class);
        } catch (JsonProcessingException err) {
            sendResponse(webSocketSession, ProduceResponse.Status.BAD_REQUEST, err.getMessage());
            return;
        }
        if (produceRequest.value() == null && produceRequest.key() == null) {
            sendResponse(
                    webSocketSession,
                    ProduceResponse.Status.BAD_REQUEST,
                    "Either key or value must be set.");
            return;
        }

        final Collection<Header> headers =
                new ArrayList<>((List<Header>) webSocketSession.getAttributes().get("headers"));
        if (produceRequest.headers() != null) {
            final Set<String> configuredHeaders =
                    headers.stream().map(Header::key).collect(Collectors.toSet());
            for (Map.Entry<String, String> messageHeader : produceRequest.headers().entrySet()) {
                if (configuredHeaders.contains(messageHeader.getKey())) {
                    sendResponse(
                            webSocketSession,
                            ProduceResponse.Status.BAD_REQUEST,
                            "Header "
                                    + messageHeader.getKey()
                                    + " is configured as parameter-level header.");
                    return;
                }
                headers.add(
                        SimpleRecord.SimpleHeader.of(
                                messageHeader.getKey(), messageHeader.getValue()));
            }
        }
        try {
            final SimpleRecord record =
                    SimpleRecord.builder()
                            .key(produceRequest.key())
                            .value(produceRequest.value())
                            .headers(headers)
                            .build();
            topicProducer.write(record).get();
            log.info("[{}] Produced record {}", webSocketSession.getId(), record);
        } catch (Throwable tt) {
            sendResponse(webSocketSession, ProduceResponse.Status.PRODUCER_ERROR, tt.getMessage());
            return;
        }

        webSocketSession.sendMessage(
                new TextMessage(mapper.writeValueAsString(ProduceResponse.OK)));
    }

    private static void sendResponse(
            WebSocketSession webSocketSession, ProduceResponse.Status status, String reason)
            throws IOException {
        webSocketSession.sendMessage(
                new TextMessage(mapper.writeValueAsString(new ProduceResponse(status, reason))));
    }

    private static TopicProducer getTopicProducer(
            WebSocketSession webSocketSession, boolean throwIfNotFound) {
        final TopicProducer topicProducer =
                (TopicProducer) webSocketSession.getAttributes().get("producer");
        if (topicProducer == null) {
            if (throwIfNotFound) {
                log.error("No producer found for session {}", webSocketSession.getId());
                throw new IllegalStateException(
                        "No producer found for session " + webSocketSession.getId());
            }
        }
        return topicProducer;
    }
}
