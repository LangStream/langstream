package com.datastax.oss.sga.apigateway.websocket.handlers;

import static com.datastax.oss.sga.apigateway.websocket.WebSocketConfig.PRODUCE_PATH;
import com.datastax.oss.sga.api.model.Gateway;
import com.datastax.oss.sga.api.model.StoredApplication;
import com.datastax.oss.sga.api.model.StreamingCluster;
import com.datastax.oss.sga.api.runner.code.Header;
import com.datastax.oss.sga.api.runner.code.SimpleRecord;
import com.datastax.oss.sga.api.runner.topics.TopicConnectionsRuntime;
import com.datastax.oss.sga.api.runner.topics.TopicProducer;
import com.datastax.oss.sga.api.storage.ApplicationStore;
import com.datastax.oss.sga.apigateway.websocket.api.ProduceRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

@Slf4j
public class ProduceHandler extends AbstractHandler {

    static final ObjectMapper mapper = new ObjectMapper();

    public ProduceHandler(ApplicationStore applicationStore) {
        super(applicationStore);
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


        final StoredApplication application = applicationStore.get(tenant, applicationId);
        Gateway selectedGateway = extractGateway(gatewayId, application);


        final Map<String, String> passedParameters = verifyParameters(webSocketSession, selectedGateway);
        final List<Header> headers = getCommonHeaders(selectedGateway, passedParameters);
        final StreamingCluster streamingCluster = application.getInstance().getInstance().streamingCluster();

        final TopicConnectionsRuntime topicConnectionsRuntime =
                TOPIC_CONNECTIONS_REGISTRY.getTopicConnectionsRuntime(streamingCluster);

        final String topicName = selectedGateway.topic();
        final TopicProducer producer =
                topicConnectionsRuntime.createProducer("ag-" + webSocketSession.getId(), streamingCluster,
                        Map.of("topic", topicName));
        producer.start();

        webSocketSession.getAttributes().put("producer", producer);
        webSocketSession.getAttributes().put("headers", Collections.unmodifiableList(headers));

        log.info("Started produced for gateway {}/{}/{} on topic {}", tenant, applicationId, gatewayId, topicName);
    }

    @Override
    public void onMessage(WebSocketSession webSocketSession, TextMessage message) throws Exception {
        final TopicProducer topicProducer = getTopicProducer(webSocketSession, true);

        final ProduceRequest produceRequest = mapper.readValue(message.getPayload(), ProduceRequest.class);
        final Collection<Header> headers =
                new ArrayList<>((List<Header>) webSocketSession.getAttributes().get("headers"));
        if (produceRequest.headers() != null) {
            headers.addAll(
                    produceRequest.headers().entrySet().stream()
                            .map(e -> SimpleRecord.SimpleHeader.of(e.getKey(), e.getValue())).toList()
            );
        }
        final ProduceHandlerRecord record =
                new ProduceHandlerRecord(produceRequest.key(), produceRequest.value(), headers);
        topicProducer.write(List.of(record));
        log.info("[{}] Produced record {}", webSocketSession.getId(), record);
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
        final TopicProducer topicProducer = getTopicProducer(webSocketSession, false);
        if (topicProducer != null) {
            topicProducer.close();
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
