package com.datastax.oss.sga.apigateway.websocket;

import com.datastax.oss.sga.apigateway.websocket.handlers.AbstractHandler;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.handler.ExceptionWebSocketHandlerDecorator;
import org.springframework.web.socket.server.HandshakeInterceptor;

@Slf4j
public class AuthenticationInterceptor implements HandshakeInterceptor {

    @Override
    public boolean beforeHandshake(ServerHttpRequest request, ServerHttpResponse response, WebSocketHandler wsHandler,
                                   Map<String, Object> attributes) throws Exception {
        final ServletServerHttpRequest httpRequest = (ServletServerHttpRequest) request;
        final String queryString = httpRequest.getServletRequest()
                .getQueryString();
        attributes.put("queryString", parseQuerystring(queryString));

        final WebSocketHandler delegate = ((ExceptionWebSocketHandlerDecorator) wsHandler)
                .getLastHandler();
        final AbstractHandler handler = (AbstractHandler) delegate;

        final AntPathMatcher antPathMatcher = new AntPathMatcher();
        final String path = httpRequest.getURI().getPath();
        final Map<String, String> vars = antPathMatcher
                .extractUriTemplateVariables(handler.path(), path);
        attributes.putAll(vars);
        handler.onBeforeHandshakeCompleted(attributes);
        return true;
    }

    @Override
    public void afterHandshake(ServerHttpRequest request, ServerHttpResponse response, WebSocketHandler wsHandler,
                               Exception exception) {
        log.info("afterHandsjare {}", wsHandler.getClass().getName());

    }


    private static Map<String, String> parseQuerystring(String queryString) {
        Map<String, String> map = new HashMap<>();
        if (queryString == null || queryString.isBlank()) {
            return map;
        }
        String[] params = queryString.split("&");
        for (String param : params) {
            try {
                String[] keyValuePair = param.split("=", 2);
                String name = URLDecoder.decode(keyValuePair[0], "UTF-8");
                if (name == "") {
                    continue;
                }
                String value = keyValuePair.length > 1 ? URLDecoder.decode(
                        keyValuePair[1], "UTF-8") : "";
                map.put(name, value);
            } catch (UnsupportedEncodingException e) {
                // ignore this parameter if it can't be decoded
            }
        }
        return map;
    }
}
