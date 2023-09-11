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
import ai.langstream.api.model.Gateway;
import ai.langstream.apigateway.config.GatewayTestAuthenticationProperties;
import ai.langstream.apigateway.websocket.handlers.AbstractHandler;
import ai.langstream.apigateway.websocket.impl.AuthenticatedGatewayRequestContextImpl;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
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

    private final GatewayAuthenticationProvider authTestProvider;

    public AuthenticationInterceptor(
            GatewayTestAuthenticationProperties testAuthenticationProperties) {
        if (testAuthenticationProperties.getType() != null) {
            authTestProvider =
                    GatewayAuthenticationProviderRegistry.loadProvider(
                            testAuthenticationProperties.getType(),
                            testAuthenticationProperties.getConfiguration());
            log.info(
                    "Loaded test authentication provider {}",
                    authTestProvider.getClass().getName());
        } else {
            authTestProvider = null;
            log.info("No test authentication provider configured");
        }
    }

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
                    handler.validateRequest(
                            vars, querystring, request.getHeaders().toSingleValueMap());

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

        if (authentication == null) {
            return Map.of();
        }

        final GatewayAuthenticationResult result;
        if (gatewayRequestContext.isTestMode()) {
            if (!authentication.isAllowTestMode()) {
                throw new AuthFailedException(
                        "Gateway "
                                + gatewayRequestContext.gateway().id()
                                + " of tenant "
                                + gatewayRequestContext.tenant()
                                + " does not allow test mode.");
            }
            if (authTestProvider == null) {
                throw new AuthFailedException("No test auth provider specified");
            }
            result = authTestProvider.authenticate(gatewayRequestContext);
        } else {
            final String provider = authentication.getProvider();
            final GatewayAuthenticationProvider authProvider =
                    GatewayAuthenticationProviderRegistry.loadProvider(
                            provider, authentication.getConfiguration());
            result = authProvider.authenticate(gatewayRequestContext);
        }
        if (result == null) {
            throw new AuthFailedException("Authentication provider returned null");
        }
        if (!result.authenticated()) {
            throw new AuthFailedException(result.reason());
        }
        return getPrincipalValues(result, gatewayRequestContext);
    }

    private Map<String, String> getPrincipalValues(
            GatewayAuthenticationResult result, GatewayRequestContext context) {
        if (!context.isTestMode()) {
            final Map<String, String> values = result.principalValues();
            if (values == null) {
                return Map.of();
            }
            return values;
        } else {
            final Map<String, String> values = new HashMap<>();
            final String principalSubject = DigestUtils.sha256Hex(context.credentials());
            final int principalNumericId = principalSubject.hashCode();
            final String principalEmail = "%s@locahost".formatted(principalSubject);

            // google
            values.putIfAbsent("subject", principalSubject);
            values.putIfAbsent("email", principalEmail);
            values.putIfAbsent("name", principalSubject);

            // github
            values.putIfAbsent("login", principalSubject);
            values.putIfAbsent("id", principalNumericId + "");
            return values;
        }
    }

    private AuthenticatedGatewayRequestContext getAuthenticatedGatewayRequestContext(
            GatewayRequestContext gatewayRequestContext,
            Map<String, String> principalValues,
            Map<String, Object> attributes) {

        return AuthenticatedGatewayRequestContextImpl.builder()
                .gatewayRequestContext(gatewayRequestContext)
                .attributes(attributes)
                .principalValues(principalValues)
                .build();
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
