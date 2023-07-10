package com.datastax.oss.sga.pulsar;

import com.datastax.oss.sga.api.runtime.ConnectionImplementation;
import com.datastax.oss.sga.api.runtime.TopicImplementation;

public record PulsarTopic(PulsarName name, String schemaName, String schemaType, String schema, String createMode)
        implements ConnectionImplementation, TopicImplementation {
}
