package com.datastax.oss.sga.apigateway.websocket;

import com.datastax.oss.sga.api.gateway.GatewayRequestContext;
import java.util.Map;

public interface AuthenticatedGatewayRequestContext extends GatewayRequestContext {

    Map<String, Object> attributes();

    Map<String, String> principalValues();

}
