package com.datastax.oss.sga.apigateway.websocket.handlers;

import static com.datastax.oss.sga.apigateway.websocket.WebSocketConfig.CONSUME_PATH;
import com.datastax.oss.sga.api.model.Gateway;
import com.datastax.oss.sga.api.model.Gateways;
import com.datastax.oss.sga.api.model.StoredApplication;
import com.datastax.oss.sga.api.model.StreamingCluster;
import com.datastax.oss.sga.api.model.TopicDefinition;
import com.datastax.oss.sga.api.runner.code.Record;
import com.datastax.oss.sga.api.runner.topics.TopicConnectionsRuntime;
import com.datastax.oss.sga.api.runner.topics.TopicConsumer;
import com.datastax.oss.sga.api.storage.ApplicationStore;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

@Slf4j
public class ConsumeHandler extends AbstractHandler {

    public ConsumeHandler(ApplicationStore applicationStore) {
        super(applicationStore);
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        super.afterConnectionEstablished(session);
        try {
            final AntPathMatcher antPathMatcher = new AntPathMatcher();
            final Map<String, String> vars =
                    antPathMatcher.extractUriTemplateVariables(CONSUME_PATH,
                            session.getUri().getPath());
            session.getAttributes().put("application", vars.get("application"));
            final String gateway = vars.get("gateway");
            session.getAttributes().put("topic", gateway);

            final String tenant = (String) session.getAttributes().get("tenant");
            final String applicationId = (String) session.getAttributes().get("application");
            setupConsumer(session, gateway, tenant, applicationId);
        } catch (Throwable tt) {
            log.error("Error while setting up consumer", tt);
            session.close();
        }
    }

    private void setupConsumer(WebSocketSession session, String gatewayId, String tenant, String applicationId)
            throws Exception {
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
        final StreamingCluster streamingCluster = application
                .getInstance()
                .getInstance().streamingCluster();

        final TopicConnectionsRuntime topicConnectionsRuntime =
                TOPIC_CONNECTIONS_REGISTRY.getTopicConnectionsRuntime(streamingCluster);


        final TopicConsumer consumer =
                topicConnectionsRuntime.createConsumer("ag-" + session.getId(), streamingCluster, Map.of("topic", topicName));
        consumer.start();


        while (true) {
            final List<Record> records = consumer.read();
            // TODO: filter out records by filters
            for (Record record : records) {
                session.sendMessage(new TextMessage(String.valueOf(record.value())));
            }
        }
    }

    @Override
    public void handleTextMessage(WebSocketSession session, TextMessage message) {

    }

}
