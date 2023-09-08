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
