package com.datastax.oss.sga.api.gateway;

import java.util.Map;

public interface GatewayAuthenticationProvider {

    String type();

    void initialize(Map<String, Object> configuration);

    GatewayAuthenticationResult authenticate(GatewayRequestContext context);
}
