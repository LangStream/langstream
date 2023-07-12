package com.datastax.oss.sga.pulsar;

import com.datastax.oss.sga.api.runtime.Connection;
import com.datastax.oss.sga.api.runtime.Topic;

public record PulsarTopic(PulsarName name, String schemaName, String schemaType, String schema, String createMode)
        implements Connection, Topic {
}
