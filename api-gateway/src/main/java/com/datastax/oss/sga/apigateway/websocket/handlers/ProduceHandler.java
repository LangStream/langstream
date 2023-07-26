package com.datastax.oss.sga.apigateway.websocket.handlers;

import static com.datastax.oss.sga.apigateway.websocket.WebSocketConfig.PRODUCE_PATH;
import static org.springframework.web.socket.CloseStatus.SERVER_ERROR;
import com.datastax.oss.sga.api.model.Gateway;
import com.datastax.oss.sga.api.model.Gateways;
import com.datastax.oss.sga.api.model.StoredApplication;
import com.datastax.oss.sga.api.model.StreamingCluster;
import com.datastax.oss.sga.api.model.TopicDefinition;
import com.datastax.oss.sga.api.runner.code.Header;
import com.datastax.oss.sga.api.runner.code.SimpleRecord;
import com.datastax.oss.sga.api.runner.topics.TopicConnectionsRuntime;
import com.datastax.oss.sga.api.runner.topics.TopicConnectionsRuntimeRegistry;
import com.datastax.oss.sga.api.runner.topics.TopicProducer;
import com.datastax.oss.sga.api.runtime.ClusterRuntimeRegistry;
import com.datastax.oss.sga.api.runtime.StreamingClusterRuntime;
import com.datastax.oss.sga.api.storage.ApplicationStore;
import com.datastax.oss.sga.apigateway.websocket.api.ProduceRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

@Slf4j
public class ProduceHandler extends AbstractHandler {

    static final ObjectMapper mapper = new ObjectMapper();

    public ProduceHandler(ApplicationStore applicationStore) {
        super(applicationStore);
    }

    private TopicProducer producer;
    private Collection<Header> headers;

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        super.afterConnectionEstablished(session);
        try {
            final AntPathMatcher antPathMatcher = new AntPathMatcher();
            final Map<String, String> vars =
                    antPathMatcher.extractUriTemplateVariables(PRODUCE_PATH,
                            session.getUri().getPath());
            session.getAttributes().put("application", vars.get("application"));
            final String gatewayId = vars.get("gateway");
            session.getAttributes().put("gateway", gatewayId);

            final String tenant = (String) session.getAttributes().get("tenant");
            final String applicationId = (String) session.getAttributes().get("application");


            final StoredApplication application = applicationStore.get(tenant, applicationId);

            final Gateways gatewaysObj = application.getInstance().getGateways();
            if (gatewaysObj == null) {
                throw new IllegalArgumentException("no gateways defined for the application");
            }
            final List<Gateway> gateways = gatewaysObj.gateways();
            if (gateways == null) {
                throw new IllegalArgumentException("no gateways defined for the application");
            }

            Gateway selectedGateway = null;


            for (Gateway gateway : gateways) {
                if (gateway.id().equals(gatewayId)) {
                    selectedGateway = gateway;
                    break;
                }
            }
            if (selectedGateway == null) {
                throw new IllegalArgumentException("gateway" + gatewayId + " is not defined in the application");
            }

            final String topicName = selectedGateway.topic();
            final List<String> requiredParameters = selectedGateway.parameters();
            final Map<String, String> querystring = (Map<String, String>)session.getAttributes().get("queryString");
            if (requiredParameters != null) {
                for (String requiredParameter : requiredParameters) {
                    if (!querystring.containsKey(requiredParameter)) {
                        throw new IllegalArgumentException("missing required parameter " + requiredParameter);
                    }
                }
            }
            headers = null;
            if (selectedGateway.produceOptions() != null && selectedGateway.produceOptions().headers() != null) {
                final Map<String, String> produceOptionsHeaders = selectedGateway.produceOptions().headers();
                headers = new ArrayList<>();
                for (Map.Entry<String, String> h : produceOptionsHeaders.entrySet()) {
                    // TODO: resolve variables
                    headers.add(SimpleRecord.SimpleHeader.of(h.getKey(), h.getValue()));
                }
            }
            final StreamingCluster streamingCluster = application
                    .getInstance()
                    .getInstance().streamingCluster();

            final TopicConnectionsRuntime topicConnectionsRuntime =
                    TOPIC_CONNECTIONS_REGISTRY.getTopicConnectionsRuntime(streamingCluster);

            producer =
                    topicConnectionsRuntime.createProducer("ag-" + session.getId(), streamingCluster,
                            Map.of("topic", topicName));
            producer.start();
            log.info("Started produced for gateway {}/{}/{} on topic {}", tenant, applicationId, gatewayId, topicName);
        } catch (Throwable tt) {
            log.error("Error while establishing connection", tt);
            session.close(SERVER_ERROR);
        }
    }

    @Override
    @SneakyThrows
    public void handleTextMessage(WebSocketSession session, TextMessage message) {
        try {
            final ProduceRequest produceRequest = mapper.readValue(message.getPayload(), ProduceRequest.class);
            Collection<Header> recordHeaders = headers == null ? null : headers;
            if (produceRequest.headers() != null) {
                if (recordHeaders == null) {
                    recordHeaders = new ArrayList<>();
                }
                recordHeaders.addAll(
                        produceRequest.headers().entrySet().stream().map(e -> SimpleRecord.SimpleHeader.of(e.getKey(), e.getValue())).toList()
                );
            }
            final ProduceHandlerRecord record =
                    new ProduceHandlerRecord(produceRequest.key(), produceRequest.value(), recordHeaders);
            producer.write(List.of(record));
            log.info("Produced record {}", record);
        } catch (Throwable tt) {
            log.error("Error while writing message", tt);
            session.close();
        }
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

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
        if (producer != null) {
            producer.close();
        }
    }
}
