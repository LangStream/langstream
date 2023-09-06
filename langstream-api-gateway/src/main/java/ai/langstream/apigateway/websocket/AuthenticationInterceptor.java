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

import ai.langstream.api.gateway.GatewayAuthenticationProvider;
import ai.langstream.api.gateway.GatewayAuthenticationProviderRegistry;
import ai.langstream.api.gateway.GatewayAuthenticationResult;
import ai.langstream.api.gateway.GatewayRequestContext;
import ai.langstream.api.model.Application;
import ai.langstream.api.model.Gateway;
import ai.langstream.apigateway.websocket.handlers.AbstractHandler;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.http.server.ServletServerHttpResponse;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.handler.ExceptionWebSocketHandlerDecorator;
import org.springframework.web.socket.server.HandshakeInterceptor;

@Slf4j
public class AuthenticationInterceptor implements HandshakeInterceptor {

    @Override
    public boolean beforeHandshake(
            ServerHttpRequest request,
            ServerHttpResponse response,
            WebSocketHandler wsHandler,
            Map<String, Object> attributes)
            throws Exception {
        final ServletServerHttpRequest httpRequest = (ServletServerHttpRequest) request;
        final ServletServerHttpResponse httpResponse = (ServletServerHttpResponse) response;
        try {
            final String queryString = httpRequest.getServletRequest().getQueryString();
            final Map<String, String> querystring = parseQuerystring(queryString);

            final WebSocketHandler delegate =
                    ((ExceptionWebSocketHandlerDecorator) wsHandler).getLastHandler();
            final AbstractHandler handler = (AbstractHandler) delegate;

            final AntPathMatcher antPathMatcher = new AntPathMatcher();
            final String path = httpRequest.getURI().getPath();
            final Map<String, String> vars =
                    antPathMatcher.extractUriTemplateVariables(handler.path(), path);
            final GatewayRequestContext gatewayRequestContext =
                    handler.validateRequest(vars, querystring);

            final Map<String, String> principalValues;
            try {
                principalValues = authenticate(gatewayRequestContext);
            } catch (AuthFailedException authFailedException) {
                log.info("Authentication failed {}", authFailedException.getMessage());
                String error = authFailedException.getMessage();
                if (error == null || error.isEmpty()) {
                    error = "unknown";
                }
                httpResponse.getServletResponse().sendError(HttpStatus.FORBIDDEN.value(), error);
                return false;
            }
            log.info("Authentication passed!");

            final AuthenticatedGatewayRequestContext authenticatedGatewayRequestContext =
                    getAuthenticatedGatewayRequestContext(
                            gatewayRequestContext, principalValues, attributes);
            attributes.put("context", authenticatedGatewayRequestContext);
            handler.onBeforeHandshakeCompleted(authenticatedGatewayRequestContext, attributes);
            return true;
        } catch (Throwable error) {
            log.info("Internal error {}", error.getMessage(), error);
            httpResponse
                    .getServletResponse()
                    .sendError(HttpStatus.INTERNAL_SERVER_ERROR.value(), error.getMessage());
            return false;
        }
    }

    private static class AuthFailedException extends Exception {
        public AuthFailedException(String message) {
            super(message);
        }
    }

    private Map<String, String> authenticate(GatewayRequestContext gatewayRequestContext)
            throws AuthFailedException {
        final Gateway.Authentication authentication =
                gatewayRequestContext.gateway().authentication();
        final Map<String, String> principalValues;
        if (authentication != null) {
            final String provider = authentication.provider();

            final GatewayAuthenticationProvider authenticationProvider =
                    GatewayAuthenticationProviderRegistry.loadProvider(
                            provider, authentication.configuration());
            final GatewayAuthenticationResult result =
                    authenticationProvider.authenticate(gatewayRequestContext);
            if (!result.authenticated()) {
                throw new AuthFailedException(result.reason());
            }
            principalValues = result.principalValues();
        } else {
            principalValues = Map.of();
        }
        if (principalValues == null) {
            return Map.of();
        }
        return principalValues;
    }

    private AuthenticatedGatewayRequestContext getAuthenticatedGatewayRequestContext(
            GatewayRequestContext gatewayRequestContext,
            Map<String, String> principalValues,
            Map<String, Object> attributes) {
        return new AuthenticatedGatewayRequestContext() {
            @Override
            public Map<String, String> principalValues() {
                return principalValues;
            }

            @Override
            public String tenant() {
                return gatewayRequestContext.tenant();
            }

            @Override
            public Map<String, Object> attributes() {
                return attributes;
            }

            @Override
            public String applicationId() {
                return gatewayRequestContext.applicationId();
            }

            @Override
            public Application application() {
                return gatewayRequestContext.application();
            }

            @Override
            public Gateway gateway() {
                return gatewayRequestContext.gateway();
            }

            @Override
            public String credentials() {
                return gatewayRequestContext.credentials();
            }

            @Override
            public Map<String, String> userParameters() {
                return gatewayRequestContext.userParameters();
            }

            @Override
            public Map<String, String> options() {
                return gatewayRequestContext.options();
            }
        };
    }

    @Override
    public void afterHandshake(
            ServerHttpRequest request,
            ServerHttpResponse response,
            WebSocketHandler wsHandler,
            Exception exception) {}

    private static Map<String, String> parseQuerystring(String queryString) {
        Map<String, String> map = new HashMap<>();
        if (queryString == null || queryString.isBlank()) {
            return map;
        }
        String[] params = queryString.split("&");
        for (String param : params) {
            String[] keyValuePair = param.split("=", 2);
            String name = URLDecoder.decode(keyValuePair[0], StandardCharsets.UTF_8);
            if ("".equals(name)) {
                continue;
            }
            String value =
                    keyValuePair.length > 1
                            ? URLDecoder.decode(keyValuePair[1], StandardCharsets.UTF_8)
                            : "";
            map.put(name, value);
        }
        return map;
    }
}
