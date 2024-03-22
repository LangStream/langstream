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

import static ai.langstream.apigateway.websocket.WebSocketConfig.PRODUCE_PATH;

import ai.langstream.api.model.Gateway;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.topics.TopicConnectionsRuntimeRegistry;
import ai.langstream.api.runtime.ClusterRuntimeRegistry;
import ai.langstream.api.storage.ApplicationStore;
import ai.langstream.apigateway.gateways.GatewayRequestHandler;
import ai.langstream.apigateway.gateways.ProduceGateway;
import ai.langstream.apigateway.gateways.TopicProducerCache;
import ai.langstream.apigateway.websocket.AuthenticatedGatewayRequestContext;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

@Slf4j
public class ProduceHandler extends AbstractHandler {

    public ProduceHandler(
            ApplicationStore applicationStore,
            TopicConnectionsRuntimeRegistry topicConnectionsRuntimeRegistry,
            ClusterRuntimeRegistry clusterRuntimeRegistry,
            TopicProducerCache topicProducerCache) {
        super(
                applicationStore,
                topicConnectionsRuntimeRegistry,
                clusterRuntimeRegistry,
                topicProducerCache);
    }

    @Override
    public String path() {
        return PRODUCE_PATH;
    }

    @Override
    public Gateway.GatewayType gatewayType() {
        return Gateway.GatewayType.produce;
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
        return new ProduceGateway.ProduceGatewayRequestValidator();
    }

    @Override
    public void onBeforeHandshakeCompleted(
            AuthenticatedGatewayRequestContext context, Map<String, Object> attributes)
            throws Exception {
        final List<Header> commonHeaders =
                ProduceGateway.getProducerCommonHeaders(
                        context.gateway().getProduceOptions(), context);
        setupProducer(context.gateway().getTopic(), commonHeaders, context);

        sendClientConnectedEvent(context);
    }

    @Override
    public void onOpen(
            WebSocketSession webSocketSession, AuthenticatedGatewayRequestContext context) {}

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
        closeProduceGateway(webSocketSession);
    }
}
