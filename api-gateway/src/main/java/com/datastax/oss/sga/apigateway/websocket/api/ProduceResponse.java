package com.datastax.oss.sga.apigateway.websocket.api;


public record ProduceResponse(Status status, String reason) {
    public static ProduceResponse OK = new ProduceResponse(Status.OK, null);

    public enum Status {
        OK,
        BAD_REQUEST,
        PRODUCER_ERROR
    }
}
