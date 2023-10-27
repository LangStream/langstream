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

import ai.langstream.api.gateway.GatewayRequestContext;
import ai.langstream.apigateway.gateways.GatewayRequestHandler;
import ai.langstream.apigateway.util.HttpUtil;
import ai.langstream.apigateway.websocket.handlers.AbstractHandler;
import java.util.Map;
import lombok.AllArgsConstructor;
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
@AllArgsConstructor
public class AuthenticationInterceptor implements HandshakeInterceptor {

    private final GatewayRequestHandler gatewayRequestHandler;

    @Override
    public boolean beforeHandshake(
            ServerHttpRequest request,
            ServerHttpResponse response,
            WebSocketHandler wsHandler,
            Map<String, Object> sessionAttributes)
            throws Exception {
        final ServletServerHttpRequest httpRequest = (ServletServerHttpRequest) request;
        final ServletServerHttpResponse httpResponse = (ServletServerHttpResponse) response;
        try {
            final String queryString = httpRequest.getServletRequest().getQueryString();
            final Map<String, String> querystring = HttpUtil.parseQuerystring(queryString);

            final WebSocketHandler delegate =
                    ((ExceptionWebSocketHandlerDecorator) wsHandler).getLastHandler();
            final AbstractHandler handler = (AbstractHandler) delegate;

            final AntPathMatcher antPathMatcher = new AntPathMatcher();
            final String path = httpRequest.getURI().getPath();
            final Map<String, String> vars =
                    antPathMatcher.extractUriTemplateVariables(handler.path(), path);
            final GatewayRequestContext gatewayRequestContext =
                    gatewayRequestHandler.validateRequest(
                            handler.tenantFromPath(vars, querystring),
                            handler.applicationIdFromPath(vars, querystring),
                            handler.gatewayFromPath(vars, querystring),
                            handler.gatewayType(),
                            querystring,
                            request.getHeaders().toSingleValueMap(),
                            handler.validator());

            final AuthenticatedGatewayRequestContext authenticatedGatewayRequestContext;
            try {
                authenticatedGatewayRequestContext =
                        gatewayRequestHandler.authenticate(gatewayRequestContext);
            } catch (GatewayRequestHandler.AuthFailedException authFailedException) {
                log.info("Authentication failed {}", authFailedException.getMessage());
                String error = authFailedException.getMessage();
                if (error == null || error.isEmpty()) {
                    error = "unknown";
                }
                httpResponse.getServletResponse().sendError(HttpStatus.UNAUTHORIZED.value(), error);
                return false;
            }
            log.debug("Authentication OK");

            sessionAttributes.put("context", authenticatedGatewayRequestContext);
            handler.onBeforeHandshakeCompleted(
                    authenticatedGatewayRequestContext,
                    authenticatedGatewayRequestContext.attributes());
            return true;
        } catch (Throwable error) {
            log.info("Internal error {}", error.getMessage(), error);
            httpResponse
                    .getServletResponse()
                    .sendError(HttpStatus.INTERNAL_SERVER_ERROR.value(), error.getMessage());
            return false;
        }
    }

    @Override
    public void afterHandshake(
            ServerHttpRequest request,
            ServerHttpResponse response,
            WebSocketHandler wsHandler,
            Exception exception) {}
}
