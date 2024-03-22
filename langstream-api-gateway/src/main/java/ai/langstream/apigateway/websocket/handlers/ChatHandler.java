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

import static ai.langstream.apigateway.websocket.WebSocketConfig.CHAT_PATH;

import ai.langstream.api.model.Gateway;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.topics.TopicConnectionsRuntimeRegistry;
import ai.langstream.api.runtime.ClusterRuntimeRegistry;
import ai.langstream.api.storage.ApplicationStore;
import ai.langstream.apigateway.gateways.ConsumeGateway;
import ai.langstream.apigateway.gateways.GatewayRequestHandler;
import ai.langstream.apigateway.gateways.ProduceGateway;
import ai.langstream.apigateway.gateways.TopicProducerCache;
import ai.langstream.apigateway.websocket.AuthenticatedGatewayRequestContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

@Slf4j
public class ChatHandler extends AbstractHandler {

    private final ExecutorService executor;

    public ChatHandler(
            ApplicationStore applicationStore,
            ExecutorService executor,
            TopicConnectionsRuntimeRegistry topicConnectionsRuntimeRegistry,
            ClusterRuntimeRegistry clusterRuntimeRegistry,
            TopicProducerCache topicProducerCache) {
        super(
                applicationStore,
                topicConnectionsRuntimeRegistry,
                clusterRuntimeRegistry,
                topicProducerCache);
        this.executor = executor;
    }

    @Override
    public String path() {
        return CHAT_PATH;
    }

    @Override
    public Gateway.GatewayType gatewayType() {
        return Gateway.GatewayType.chat;
    }

    @Override
    public String tenantFromPath(Map<String, String> parsedPath, Map<String, String> queryString) {
        return parsedPath.get("tenant");
    }

    @Override
    public String applicationIdFromPath(
            Map<String, String> parsedPath, Map<String, String> queryString) {
        return parsedPath.get("application");
    }

    @Override
    public String gatewayFromPath(Map<String, String> parsedPath, Map<String, String> queryString) {
        return parsedPath.get("gateway");
    }

    @Override
    public GatewayRequestHandler.GatewayRequestValidator validator() {
        return new GatewayRequestHandler.GatewayRequestValidator() {
            @Override
            public List<String> getAllRequiredParameters(Gateway gateway) {
                List<String> parameters = gateway.getParameters();
                if (parameters == null) {
                    parameters = new ArrayList<>();
                }
                if (gateway.getChatOptions() != null
                        && gateway.getChatOptions().getHeaders() != null) {
                    for (Gateway.KeyValueComparison header :
                            gateway.getChatOptions().getHeaders()) {
                        if (header.valueFromParameters() != null) {
                            parameters.add(header.valueFromParameters());
                        }
                    }
                }
                return parameters;
            }

            @Override
            public void validateOptions(Map<String, String> options) {
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
        };
    }

    @Override
    public void onBeforeHandshakeCompleted(
            AuthenticatedGatewayRequestContext context, Map<String, Object> attributes)
            throws Exception {

        try {
            setupReader(context);
        } catch (Exception ex) {
            log.error("Error setting up reader", ex);
            throw ex;
        }
        try {
            setupProducer(context);
        } catch (Exception ex) {
            log.error("Error setting up producer", ex);
            closeConsumeGateway(context);
            throw ex;
        }

        sendClientConnectedEvent(context);
    }

    private void setupProducer(AuthenticatedGatewayRequestContext context) throws Exception {
        final List<Header> commonHeaders =
                ProduceGateway.getProducerCommonHeaders(
                        context.gateway().getChatOptions(), context);

        setupProducer(
                context.gateway().getChatOptions().getQuestionsTopic(), commonHeaders, context);
    }

    private void setupReader(AuthenticatedGatewayRequestContext context) throws Exception {
        final Gateway.ChatOptions chatOptions = context.gateway().getChatOptions();
        final List<Function<Record, Boolean>> messageFilters =
                ConsumeGateway.createMessageFilters(
                        chatOptions.getHeaders(),
                        context.userParameters(),
                        context.principalValues());

        setupReader(chatOptions.getAnswersTopic(), messageFilters, context);
    }

    @Override
    public void onOpen(
            WebSocketSession webSocketSession, AuthenticatedGatewayRequestContext context) {
        startReadingMessages(webSocketSession, executor);
    }

    @Override
    public void onMessage(
            WebSocketSession webSocketSession,
            AuthenticatedGatewayRequestContext context,
            TextMessage message)
            throws Exception {
        produceMessage(webSocketSession, message);
    }

    @Override
    public void onClose(
            WebSocketSession webSocketSession,
            AuthenticatedGatewayRequestContext context,
            CloseStatus status) {
        closeConsumeGateway(webSocketSession);
        closeProduceGateway(webSocketSession);
    }
}
