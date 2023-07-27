package com.datastax.oss.sga.apigateway.websocket.handlers;

import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.Gateway;
import com.datastax.oss.sga.api.model.Gateways;
import com.datastax.oss.sga.api.model.StoredApplication;
import com.datastax.oss.sga.api.runner.topics.TopicConnectionsRuntimeRegistry;
import com.datastax.oss.sga.api.storage.ApplicationStore;
import com.datastax.oss.sga.impl.common.ApplicationPlaceholderResolver;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    protected static final ObjectMapper mapper = new ObjectMapper();
    protected static final TopicConnectionsRuntimeRegistry TOPIC_CONNECTIONS_REGISTRY =
            new TopicConnectionsRuntimeRegistry();
    protected final ApplicationStore applicationStore;

    public abstract String path();

    public void onBeforeHandshakeCompleted(Map<String, Object> attributes) throws Exception {};

    public abstract void onOpen(WebSocketSession webSocketSession) throws Exception;

    public abstract void onMessage(WebSocketSession webSocketSession, TextMessage message) throws Exception;

    public abstract void onClose(WebSocketSession webSocketSession, CloseStatus status) throws Exception;

    interface RequestDetails {
        Map<String, String> getUserParameters();

        Map<String, String> getOptions();
    }


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
        try {
            session.close(status.withReason(throwable.getMessage()));
        } finally {
            closeCloseableResources(session);
        }
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
        closeCloseableResources(session);
    }


    @Override
    public boolean supportsPartialMessages() {
        return true;
    }

    protected Application getResolvedApplication(String tenant, String applicationId) {
        final Application application = applicationStore.getSpecs(tenant, applicationId);
        if (application == null) {
            throw new IllegalArgumentException("application " + applicationId + " not found");
        }
        application.setSecrets(applicationStore.getSecrets(tenant, applicationId));
        return ApplicationPlaceholderResolver
                .resolvePlaceholders(application);
    }

    protected Gateway extractGateway(String gatewayId, Application application, Gateway.GatewayType type) {
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
            if (gateway.id().equals(gatewayId) && type == gateway.type()) {
                selectedGateway = gateway;
                break;
            }
        }
        if (selectedGateway == null) {
            throw new IllegalArgumentException(
                    "gateway " + gatewayId + " of type " + type + " is not defined in the application");
        }
        return selectedGateway;
    }

    abstract void validateOptions(Map<String, String> options);


    protected RequestDetails validateQueryStringAndOptions(Map<String, String> queryString, Gateway gateway) {
        Map<String, String> options = new HashMap<>();
        Map<String, String> userParameters = new HashMap<>();


        for (Map.Entry<String, String> entry : queryString.entrySet()) {
            if (entry.getKey().startsWith("option:")) {
                options.put(entry.getKey().substring("option:".length()), entry.getValue());
            } else if (entry.getKey().startsWith("param:")) {
                userParameters.put(entry.getKey().substring("param:".length()), entry.getValue());
            } else {
                throw new IllegalArgumentException("invalid query parameter " + entry.getKey() + ". "
                        + "To specify a gateway parameter, use the format param:<parameter_name>."
                        + "To specify a option, use the format option:<option_name>.");
            }
        }

        final List<String> requiredParameters = gateway.parameters();
        Set<String> allUserParameterKeys = new HashSet<>(userParameters.keySet());
        if (requiredParameters != null) {
            for (String requiredParameter : requiredParameters) {
                final String value = userParameters.get(requiredParameter);
                if (StringUtils.isBlank(value)) {
                    throw new IllegalArgumentException("missing required parameter " + requiredParameter);
                }
                allUserParameterKeys.remove(requiredParameter);
            }
        }
        if (!allUserParameterKeys.isEmpty()) {
            throw new IllegalArgumentException("unknown parameters: " + allUserParameterKeys);
        }
        validateOptions(options);
        return new RequestDetails() {
            @Override
            public Map<String, String> getUserParameters() {
                return userParameters;
            }

            @Override
            public Map<String, String> getOptions() {
                return options;
            }
        };
    }


    protected void recordCloseableResource(WebSocketSession webSocketSession, AutoCloseable... closeables) {
        List<AutoCloseable> currentCloseable =
                (List<AutoCloseable>) webSocketSession.getAttributes().get("closeables");

        if (currentCloseable == null) {
            currentCloseable = new ArrayList<>();
        }
        for (AutoCloseable closeable : closeables) {
            currentCloseable.add(closeable);
        }
        webSocketSession.getAttributes().put("closeables", currentCloseable);
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
}
