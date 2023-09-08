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

import ai.langstream.api.gateway.GatewayAdminRequestContext;
import ai.langstream.api.model.Application;
import ai.langstream.api.model.Gateway;
import java.util.Map;
import lombok.Builder;

@Builder
public class GatewayRequestContextImpl implements GatewayAdminRequestContext {

    private final String tenant;
    private final String applicationId;
    private final Application application;
    private final Gateway gateway;
    private final String credentials;
    private final String adminCredentials;
    private final String adminCredentialsType;
    private final Map<String, String> adminCredentialsInputs;
    private final Map<String, String> userParameters;
    private final Map<String, String> options;
    private final Map<String, String> httpHeaders;

    @Override
    public String tenant() {
        return tenant;
    }

    @Override
    public String applicationId() {
        return applicationId;
    }

    @Override
    public Application application() {
        return application;
    }

    @Override
    public Gateway gateway() {
        return gateway;
    }

    @Override
    public String credentials() {
        if (isAdminRequest()) {
            return adminCredentials;
        }
        return credentials;
    }

    public boolean isAdminRequest() {
        return adminCredentials != null;
    }

    @Override
    public Map<String, String> userParameters() {
        return userParameters;
    }

    @Override
    public Map<String, String> options() {
        return options;
    }

    @Override
    public Map<String, String> httpHeaders() {
        return httpHeaders;
    }

    @Override
    public String adminCredentialsType() {
        if (isAdminRequest()) {
            return adminCredentialsType;
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, String> adminCredentialsInputs() {
        return isAdminRequest() ? adminCredentialsInputs : Map.of();
    }
}
