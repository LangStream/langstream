package com.datastax.oss.sga.apigateway.websocket.api;

import java.util.Map;

public record ProduceRequest(Object key, Object value, Map<String, String> headers) {
}
