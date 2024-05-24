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
package ai.langstream.apigateway.websocket;

import ai.langstream.api.runner.topics.TopicConnectionsRuntimeRegistry;
import ai.langstream.api.runtime.ClusterRuntimeRegistry;
import ai.langstream.api.storage.ApplicationStore;
import ai.langstream.apigateway.gateways.GatewayRequestHandler;
import ai.langstream.apigateway.gateways.TopicProducerCache;
import ai.langstream.apigateway.runner.TopicConnectionsRuntimeProviderBean;
import ai.langstream.apigateway.websocket.handlers.ChatHandler;
import ai.langstream.apigateway.websocket.handlers.ConsumeHandler;
import ai.langstream.apigateway.websocket.handlers.ProduceHandler;
import jakarta.annotation.PreDestroy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.web.socket.server.standard.ServletServerContainerFactoryBean;
import org.springframework.web.socket.server.support.HttpSessionHandshakeInterceptor;

@EnableWebSocket
@Configuration
@Slf4j
@AllArgsConstructor
public class WebSocketConfig implements WebSocketConfigurer {

    public static final String CONSUME_PATH = "/v1/consume/{tenant}/{application}/{gateway}";
    public static final String PRODUCE_PATH = "/v1/produce/{tenant}/{application}/{gateway}";
    public static final String CHAT_PATH = "/v1/chat/{tenant}/{application}/{gateway}";

    private final ApplicationStore applicationStore;
    private final TopicConnectionsRuntimeProviderBean topicConnectionsRuntimeRegistryProvider;
    private final ClusterRuntimeRegistry clusterRuntimeRegistry;
    private final GatewayRequestHandler gatewayRequestHandler;
    private final TopicProducerCache topicProducerCache;
    private final ExecutorService consumeThreadPool =
            Executors.newCachedThreadPool(
                    new BasicThreadFactory.Builder().namingPattern("ws-consume-%d").build());

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        TopicConnectionsRuntimeRegistry topicConnectionsRuntimeRegistry =
                topicConnectionsRuntimeRegistryProvider.getTopicConnectionsRuntimeRegistry();
        registry.addHandler(
                        new ConsumeHandler(
                                applicationStore,
                                consumeThreadPool,
                                topicConnectionsRuntimeRegistry,
                                clusterRuntimeRegistry,
                                topicProducerCache),
                        CONSUME_PATH)
                .addHandler(
                        new ProduceHandler(
                                applicationStore,
                                topicConnectionsRuntimeRegistry,
                                clusterRuntimeRegistry,
                                topicProducerCache),
                        PRODUCE_PATH)
                .addHandler(
                        new ChatHandler(
                                applicationStore,
                                consumeThreadPool,
                                topicConnectionsRuntimeRegistry,
                                clusterRuntimeRegistry,
                                topicProducerCache),
                        CHAT_PATH)
                .setAllowedOrigins("*")
                .addInterceptors(
                        new HttpSessionHandshakeInterceptor(),
                        new AuthenticationInterceptor(gatewayRequestHandler));
    }

    @Bean
    public ServletServerContainerFactoryBean createWebSocketContainer() {
        return new ServletServerContainerFactoryBean();
    }

    @PreDestroy
    public void onDestroy() {
        log.info("Shutting down WebSocket");
        consumeThreadPool.shutdownNow();
        clusterRuntimeRegistry.close();
        topicProducerCache.close();
    }
}
