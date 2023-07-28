package com.datastax.oss.sga.api.gateway;

import java.util.Map;

public interface GatewayAuthenticationResult {

    static GatewayAuthenticationResult authenticationSuccessful(Map<String, String> principalValues) {
        return new GatewayAuthenticationResult() {
            @Override
            public boolean authenticated() {
                return true;
            }

            @Override
            public String reason() {
                return null;
            }

            @Override
            public Map<String, String> principalValues() {
                return principalValues;
            }
        };
    }

    static GatewayAuthenticationResult authenticationFailed(String reason) {
        return new GatewayAuthenticationResult() {
            @Override
            public boolean authenticated() {
                return false;
            }

            @Override
            public String reason() {
                return reason;
            }

            @Override
            public Map<String, String> principalValues() {
                return null;
            }
        };
    }

    boolean authenticated();

    String reason();

    Map<String, String> principalValues();

}
