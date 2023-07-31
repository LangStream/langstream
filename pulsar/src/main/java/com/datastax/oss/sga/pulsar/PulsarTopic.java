package com.datastax.oss.sga.pulsar;

import com.datastax.oss.sga.api.model.SchemaDefinition;
import com.datastax.oss.sga.api.runtime.Connection;
import com.datastax.oss.sga.api.runtime.Topic;

public record PulsarTopic(PulsarName name, int partitions,  SchemaDefinition keySchema, SchemaDefinition valueSchema, String createMode, boolean implicit)
        implements Connection, Topic {

    @Override
    public String topicName() {
        return name.toPulsarName();
    }

    @Override
    public boolean implicit() {
        return this.implicit;
    }
}
