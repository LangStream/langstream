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

import ai.langstream.apigateway.websocket.handlers.ConsumeHandler;
import ai.langstream.apigateway.websocket.handlers.ProduceHandler;
import ai.langstream.api.storage.ApplicationStore;
import jakarta.annotation.PreDestroy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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

    private final ApplicationStore applicationStore;
    private final ExecutorService consumeThreadPool = Executors.newCachedThreadPool();


    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(new ConsumeHandler(applicationStore, consumeThreadPool), CONSUME_PATH)
                .addHandler(new ProduceHandler(applicationStore), PRODUCE_PATH)
                .setAllowedOrigins("*")
                .addInterceptors(
                        new HttpSessionHandshakeInterceptor(),
                        new AuthenticationInterceptor());
    }

    @Bean
    public ServletServerContainerFactoryBean createWebSocketContainer() {
        return new ServletServerContainerFactoryBean();
    }

    @PreDestroy
    public void onDestroy() {
        consumeThreadPool.shutdown();
    }

}
