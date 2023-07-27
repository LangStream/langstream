package com.datastax.oss.sga.apigateway.websocket.handlers;

import static com.datastax.oss.sga.apigateway.websocket.WebSocketConfig.PRODUCE_PATH;
import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.Gateway;
import com.datastax.oss.sga.api.model.StoredApplication;
import com.datastax.oss.sga.api.model.StreamingCluster;
import com.datastax.oss.sga.api.runner.code.Header;
import com.datastax.oss.sga.api.runner.code.SimpleRecord;
import com.datastax.oss.sga.api.runner.topics.TopicConnectionsRuntime;
import com.datastax.oss.sga.api.runner.topics.TopicProducer;
import com.datastax.oss.sga.api.storage.ApplicationStore;
import com.datastax.oss.sga.apigateway.websocket.api.ProduceRequest;
import com.datastax.oss.sga.apigateway.websocket.api.ProduceResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

@Slf4j
public class ProduceHandler extends AbstractHandler {

    public ProduceHandler(ApplicationStore applicationStore) {
        super(applicationStore);
    }

    @Override
    public String path() {
        return PRODUCE_PATH;
    }

    @Override
    public void onOpen(WebSocketSession webSocketSession) throws Exception {
        final String tenant = (String) webSocketSession.getAttributes().get("tenant");

        final AntPathMatcher antPathMatcher = new AntPathMatcher();
        final Map<String, String> vars =
                antPathMatcher.extractUriTemplateVariables(PRODUCE_PATH,
                        webSocketSession.getUri().getPath());
        final String gatewayId = vars.get("gateway");
        final String applicationId = vars.get("application");


        final Application application = getResolvedApplication(tenant, applicationId);
        Gateway selectedGateway = extractGateway(gatewayId, application, Gateway.GatewayType.produce);


        final RequestDetails requestDetails = validateQueryStringAndOptions(
                (Map<String, String>) webSocketSession.getAttributes().get("queryString"), selectedGateway);
        final List<Header> headers = getCommonHeaders(selectedGateway, requestDetails.getUserParameters());
        final StreamingCluster streamingCluster = application.getInstance().streamingCluster();

        final TopicConnectionsRuntime topicConnectionsRuntime =
                TOPIC_CONNECTIONS_REGISTRY.getTopicConnectionsRuntime(streamingCluster);

        final String topicName = selectedGateway.topic();
        final TopicProducer producer =
                topicConnectionsRuntime.createProducer("ag-" + webSocketSession.getId(), streamingCluster,
                        Map.of("topic", topicName));
        recordCloseableResource(webSocketSession, producer);
        producer.start();

        webSocketSession.getAttributes().put("producer", producer);
        webSocketSession.getAttributes().put("headers", Collections.unmodifiableList(headers));

        log.info("Started produced for gateway {}/{}/{} on topic {}", tenant, applicationId, gatewayId, topicName);
    }

    @Override
    public void onMessage(WebSocketSession webSocketSession, TextMessage message) throws Exception {
        final TopicProducer topicProducer = getTopicProducer(webSocketSession, true);
        final ProduceRequest produceRequest;
        try {
            produceRequest = mapper.readValue(message.getPayload(), ProduceRequest.class);
        } catch (JsonProcessingException err) {
            sendResponse(webSocketSession, ProduceResponse.Status.BAD_REQUEST, err.getMessage());
            return;
        }
        if (produceRequest.value() == null && produceRequest.key() == null) {
            sendResponse(webSocketSession, ProduceResponse.Status.BAD_REQUEST, "Either key or value must be set.");
            return;
        }

        final Collection<Header> headers =
                new ArrayList<>((List<Header>) webSocketSession.getAttributes().get("headers"));
        if (produceRequest.headers() != null) {
            final Set<String> configuredHeaders = headers.stream().map(Header::key).collect(Collectors.toSet());
            log.info("configuredHeaders: {} passed {}", configuredHeaders, produceRequest.headers());
            for (Map.Entry<String, String> messageHeader : produceRequest.headers().entrySet()) {
                if (configuredHeaders.contains(messageHeader.getKey())) {
                    sendResponse(webSocketSession, ProduceResponse.Status.BAD_REQUEST,
                            "Header " + messageHeader.getKey() +
                                    " is configured as parameter-level header.");
                    return;
                }
                headers.add(SimpleRecord.SimpleHeader.of(messageHeader.getKey(), messageHeader.getValue()));
            }
        }
        try {
            final ProduceHandlerRecord record =
                    new ProduceHandlerRecord(produceRequest.key(), produceRequest.value(), headers);
            topicProducer.write(List.of(record));
            log.info("[{}] Produced record {}", webSocketSession.getId(), record);
        } catch (Throwable tt) {
            sendResponse(webSocketSession, ProduceResponse.Status.PRODUCER_ERROR,
                    tt.getMessage());
            return;
        }

        webSocketSession.sendMessage(new TextMessage(mapper.writeValueAsString(ProduceResponse.OK)));
    }

    private void sendResponse(WebSocketSession webSocketSession, ProduceResponse.Status status, String reason)
            throws IOException {
        webSocketSession.sendMessage(new TextMessage(mapper.writeValueAsString(new ProduceResponse(status, reason))));
    }

    private TopicProducer getTopicProducer(WebSocketSession webSocketSession, boolean throwIfNotFound) {
        final TopicProducer topicProducer = (TopicProducer) webSocketSession.getAttributes().get("producer");
        if (topicProducer == null) {
            if (throwIfNotFound) {
                log.error("No producer found for session {}", webSocketSession.getId());
                throw new IllegalStateException("No producer found for session " + webSocketSession.getId());
            }
        }
        return topicProducer;
    }

    @Override
    public void onClose(WebSocketSession webSocketSession, CloseStatus status) throws Exception {
    }

    @Override
    void validateOptions(Map<String, String> options) {
        for (Map.Entry<String, String> option : options.entrySet()) {
            switch (option.getKey()) {
                default:
                    throw new IllegalArgumentException("Unknown option " + option.getKey());
            }
        }
    }

    private List<Header> getCommonHeaders(Gateway selectedGateway, Map<String, String> passedParameters) {
        final List<Header> headers = new ArrayList<>();
        if (selectedGateway.produceOptions() != null && selectedGateway.produceOptions().headers() != null) {
            final List<Gateway.KeyValueComparison> headersConfig = selectedGateway.produceOptions().headers();
            for (Gateway.KeyValueComparison mapping : headersConfig) {
                if (mapping.key() == null || mapping.key().isEmpty()) {
                    throw new IllegalArgumentException("Header key cannot be empty");
                }
                String value = mapping.value();
                if (value == null && mapping.valueFromParameters() != null) {
                    value = passedParameters.get(mapping.valueFromParameters());
                }
                if (value == null) {
                    throw new IllegalArgumentException(
                            "Value cannot be empty (tried 'value' and 'valueFromParameters')");
                }

                headers.add(SimpleRecord.SimpleHeader.of(
                        mapping.key(),
                        value)
                );
            }
        }
        return headers;
    }


    @AllArgsConstructor
    @ToString
    private static class ProduceHandlerRecord implements com.datastax.oss.sga.api.runner.code.Record {
        private final Object key;
        private final Object value;
        private final Collection<Header> headers;

        @Override
        public Object key() {
            return key;
        }

        @Override
        public Object value() {
            return value;
        }

        @Override
        public String origin() {
            return null;
        }

        @Override
        public Long timestamp() {
            return null;
        }

        @Override
        public Collection<Header> headers() {
            return headers;
        }

    }
}
