package com.datastax.oss.sga.apigateway.websocket;

import com.datastax.oss.sga.api.storage.ApplicationStore;
import com.datastax.oss.sga.apigateway.websocket.handlers.ConsumeHandler;
import com.datastax.oss.sga.apigateway.websocket.handlers.ProduceHandler;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import org.springframework.web.socket.server.standard.ServletServerContainerFactoryBean;
import org.springframework.web.socket.server.support.HttpSessionHandshakeInterceptor;

@EnableWebSocket
@Configuration
@Slf4j
@AllArgsConstructor
public class WebSocketConfig implements WebSocketConfigurer {

    public static final String CONSUME_PATH = "/v1/consume/{tenant}/{application}/{topic}";
    public static final String PRODUCE_PATH = "/v1/produce/{tenant}/{application}/{topic}";

    private final ApplicationStore applicationStore;


    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(new ConsumeHandler(applicationStore), CONSUME_PATH)
                .addHandler(new ProduceHandler(applicationStore), PRODUCE_PATH)
                .addInterceptors(
                        new HttpSessionHandshakeInterceptor(),
                        new AuthenticationInterceptor());
    }

    @Bean
    public ServletServerContainerFactoryBean createWebSocketContainer() {
        ServletServerContainerFactoryBean container = new ServletServerContainerFactoryBean();
        container.setMaxTextMessageBufferSize(8192);
        container.setMaxBinaryMessageBufferSize(8192);
        return container;
    }

}