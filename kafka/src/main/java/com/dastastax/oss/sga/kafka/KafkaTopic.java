package com.dastastax.oss.sga.kafka;

import com.datastax.oss.sga.api.runtime.ConnectionImplementation;
import com.datastax.oss.sga.api.runtime.TopicImplementation;

public record KafkaTopic(String name, String schemaName, String schemaType, String schema, String createMode)
        implements ConnectionImplementation, TopicImplementation {
}
