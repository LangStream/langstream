package com.datastax.oss.sga.pulsar;

public record PulsarTopic(PulsarName name, String schemaName, String schemaType, String schema, String createMode) {
}
