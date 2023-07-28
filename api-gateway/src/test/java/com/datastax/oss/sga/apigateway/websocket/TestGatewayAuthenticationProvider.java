package com.datastax.oss.sga.apigateway.websocket;

import com.datastax.oss.sga.api.gateway.GatewayAuthenticationProvider;
import com.datastax.oss.sga.api.gateway.GatewayAuthenticationResult;
import com.datastax.oss.sga.api.gateway.GatewayRequestContext;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestGatewayAuthenticationProvider implements GatewayAuthenticationProvider {

    @Override
    public String type() {
        return "test-auth";
    }

    @Override
    public void initialize(Map<String, Object> configuration) {
    }

    @Override
    public GatewayAuthenticationResult authenticate(GatewayRequestContext context) {
        log.info("Authenticating {}", context.credentials());
        if (context.credentials().equals("test-user-password")) {
            return GatewayAuthenticationResult.authenticationSuccessful(Map.of("user-id", "test-user"));
        } else {
            return GatewayAuthenticationResult.authenticationFailed("Invalid credentials");
        }
    }
}
