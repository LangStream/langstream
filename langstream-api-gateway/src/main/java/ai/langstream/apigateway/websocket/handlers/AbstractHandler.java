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

import ai.langstream.api.model.ApplicationSpecs;
import ai.langstream.apigateway.websocket.AuthenticatedGatewayRequestContext;
import ai.langstream.api.gateway.GatewayRequestContext;
import ai.langstream.api.model.Application;
import ai.langstream.api.model.Gateway;
import ai.langstream.api.model.Gateways;
import ai.langstream.api.runner.topics.TopicConnectionsRuntimeRegistry;
import ai.langstream.api.storage.ApplicationStore;
import ai.langstream.impl.common.ApplicationPlaceholderResolver;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;
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

    abstract Gateway.GatewayType gatewayType();

    abstract String tenantFromPath(Map<String, String> parsedPath, Map<String, String> queryString);

    abstract String applicationIdFromPath(Map<String, String> parsedPath, Map<String, String> queryString);

    abstract String gatewayFromPath(Map<String, String> parsedPath, Map<String, String> queryString);


    public void onBeforeHandshakeCompleted(AuthenticatedGatewayRequestContext gatewayRequestContext) throws Exception {}

    abstract void onOpen(WebSocketSession webSocketSession, AuthenticatedGatewayRequestContext gatewayRequestContext) throws Exception;

    abstract void onMessage(WebSocketSession webSocketSession, AuthenticatedGatewayRequestContext gatewayRequestContext, TextMessage message) throws Exception;

    abstract void onClose(WebSocketSession webSocketSession, AuthenticatedGatewayRequestContext gatewayRequestContext, CloseStatus status) throws Exception;

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
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        try {
            onMessage(session, getContext(session), message);
        } catch (Throwable throwable) {
            log.error("[{}] error while opening websocket", session.getId(), throwable);
            closeSession(session, throwable);
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
        super.afterConnectionClosed(session, status);
        try {
            onClose(session, getContext(session), status);
        } catch (Throwable throwable) {
            log.error("[{}] error while closing websocket", session.getId(), throwable);
        }
        closeCloseableResources(session);
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
        return ApplicationPlaceholderResolver
                .resolvePlaceholders(application);
    }

    private Gateway extractGateway(String gatewayId, Application application, Gateway.GatewayType type) {
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

    public GatewayRequestContext validateRequest(Map<String, String> pathVars, Map<String, String> queryString) {
        Map<String, String> options = new HashMap<>();
        Map<String, String> userParameters = new HashMap<>();

        final String credentials = queryString.remove("credentials");

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

        final String tenant = tenantFromPath(pathVars, queryString);
        final String applicationId = applicationIdFromPath(pathVars, queryString);
        final String gatewayId = gatewayFromPath(pathVars, queryString);

        final Application application = getResolvedApplication(tenant, applicationId);
        final Gateway.GatewayType type = gatewayType();
        final Gateway gateway = extractGateway(gatewayId, application, type);


        final List<String> requiredParameters = gateway.parameters();
        Set<String> allUserParameterKeys = new HashSet<>(userParameters.keySet());
        if (requiredParameters != null) {
            for (String requiredParameter : requiredParameters) {
                final String value = userParameters.get(requiredParameter);
                if (!StringUtils.hasText(value)) {
                    throw new IllegalArgumentException("missing required parameter " + requiredParameter);
                }
                allUserParameterKeys.remove(requiredParameter);
            }
        }
        if (!allUserParameterKeys.isEmpty()) {
            throw new IllegalArgumentException("unknown parameters: " + allUserParameterKeys);
        }
        validateOptions(options);




        return new GatewayRequestContext() {

            @Override
            public String tenant() {
                return tenant;
            }

            @Override
            public String applicationId() {
                return applicationId;
            }

            @Override
            public Application application() {
                return application;
            }

            @Override
            public Gateway gateway() {
                return gateway;
            }

            @Override
            public String credentials() {
                return credentials;
            }

            @Override
            public Map<String, String> userParameters() {
                return userParameters;
            }

            @Override
            public Map<String, String> options() {
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
        Collections.addAll(currentCloseable, closeables);
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
