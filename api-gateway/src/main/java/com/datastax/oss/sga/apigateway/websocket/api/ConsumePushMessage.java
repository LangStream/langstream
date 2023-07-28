package com.datastax.oss.sga.apigateway.websocket.api;

import java.util.Map;

public record ConsumePushMessage(Record record, String offset) {
    public record Record(Object key, Object value, Map<String, String> headers) {
    }

}
