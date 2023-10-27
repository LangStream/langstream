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
package ai.langstream.apigateway.websocket.impl;

import ai.langstream.api.gateway.GatewayRequestContext;
import ai.langstream.api.model.Application;
import ai.langstream.api.model.Gateway;
import ai.langstream.apigateway.websocket.AuthenticatedGatewayRequestContext;
import java.util.Map;
import lombok.Builder;

@Builder
public class AuthenticatedGatewayRequestContextImpl implements AuthenticatedGatewayRequestContext {

    private final String sessionId;
    private final GatewayRequestContext gatewayRequestContext;
    private final Map<String, Object> attributes;
    private final Map<String, String> principalValues;

    @Override
    public Map<String, Object> attributes() {
        return attributes;
    }

    @Override
    public Map<String, String> principalValues() {
        return principalValues;
    }

    @Override
    public String tenant() {
        return gatewayRequestContext.tenant();
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
    public boolean isTestMode() {
        return gatewayRequestContext.isTestMode();
    }

    @Override
    public Map<String, String> userParameters() {
        return gatewayRequestContext.userParameters();
    }

    @Override
    public Map<String, String> options() {
        return gatewayRequestContext.options();
    }

    @Override
    public Map<String, String> httpHeaders() {
        return gatewayRequestContext.httpHeaders();
    }
}
