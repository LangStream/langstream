package com.datastax.oss.sga.apigateway.websocket.handlers;

import com.datastax.oss.sga.api.model.Gateway;
import com.datastax.oss.sga.api.model.Gateways;
import com.datastax.oss.sga.api.model.StoredApplication;
import com.datastax.oss.sga.api.runner.topics.TopicConnectionsRuntimeRegistry;
import com.datastax.oss.sga.api.storage.ApplicationStore;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

@AllArgsConstructor
@Slf4j
public abstract class AbstractHandler extends TextWebSocketHandler {
    protected static final TopicConnectionsRuntimeRegistry TOPIC_CONNECTIONS_REGISTRY = new TopicConnectionsRuntimeRegistry();
    protected final ApplicationStore applicationStore;


    public abstract void onOpen(WebSocketSession webSocketSession) throws Exception;

    public abstract void onMessage(WebSocketSession webSocketSession, TextMessage message) throws Exception;

    public abstract void onClose(WebSocketSession webSocketSession, CloseStatus status) throws Exception;


    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        super.afterConnectionEstablished(session);
        try {
            onOpen(session);
        } catch (Throwable throwable) {
            log.error("[{}] error while opening websocket", session.getId(), throwable);
            closeSession(session, throwable);
        }
    }

    private void closeSession(WebSocketSession session, Throwable throwable) throws IOException {
        CloseStatus status = CloseStatus.SERVER_ERROR;
        if (throwable instanceof IllegalArgumentException) {
            status = CloseStatus.POLICY_VIOLATION;
        }
        session.close(status.withReason(throwable.getMessage()));
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        try {
            onMessage(session, message);
        } catch (Throwable throwable) {
            log.error("[{}] error while opening websocket", session.getId(), throwable);
            closeSession(session, throwable);
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
        super.afterConnectionEstablished(session);
        try {
            onClose(session, status);
        } catch (Throwable throwable) {
            log.error("[{}] error while closing websocket", session.getId(), throwable);
        }
    }


    @Override
    public boolean supportsPartialMessages() {
        return true;
    }

    protected Gateway extractGateway(String gatewayId, StoredApplication application) {
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
        return selectedGateway;
    }


    protected Map<String, String> verifyParameters(WebSocketSession session, Gateway selectedGateway) {
        final List<String> requiredParameters = selectedGateway.parameters();
        final Map<String, String> passedParameters = new HashMap<>();
        final Map<String, String> querystring = (Map<String, String>) session.getAttributes().get("queryString");
        if (requiredParameters != null) {
            for (String requiredParameter : requiredParameters) {
                final String value = querystring.get(requiredParameter);
                if (StringUtils.isBlank(value)) {
                    throw new IllegalArgumentException("missing required parameter " + requiredParameter);
                }
                passedParameters.put(requiredParameter, value);
            }
        }
        return passedParameters;
    }


}
