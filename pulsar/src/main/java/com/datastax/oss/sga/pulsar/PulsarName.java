package com.datastax.oss.sga.pulsar;

public record PulsarName(String tenant, String namespace, String name) {
}
