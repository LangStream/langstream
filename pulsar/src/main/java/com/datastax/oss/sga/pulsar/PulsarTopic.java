package com.datastax.oss.sga.pulsar;

import com.datastax.oss.sga.api.runtime.ConnectionImplementation;

public record PulsarTopic(PulsarName name, String schemaName, String schemaType, String schema, String createMode)
        implements ConnectionImplementation {
}
