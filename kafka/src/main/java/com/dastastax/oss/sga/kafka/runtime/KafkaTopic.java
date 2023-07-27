package com.dastastax.oss.sga.kafka.runtime;

import com.datastax.oss.sga.api.model.SchemaDefinition;
import com.datastax.oss.sga.api.runtime.Connection;
import com.datastax.oss.sga.api.runtime.Topic;

import java.util.HashMap;
import java.util.Map;

public record KafkaTopic(String name, int partitions, int replicationFactor, SchemaDefinition keySchema, SchemaDefinition valueSchema, String createMode,
                         Map<String, Object> config)
        implements Connection, Topic {
    public Map<String,Object> createConsumerConfiguration() {
        Map<String, Object> configuration = new HashMap<>();


        // this is for the Agent
        configuration.put("topic", name);

        // TODO: handle schema

        return configuration;
    }

    public Map<String,Object> createProducerConfiguration() {
        Map<String, Object> configuration = new HashMap<>();

        // this is for the Agent
        configuration.put("topic", name);

        // TODO: handle schema

        return configuration;
    }

    @Override
    public String topicName() {
        return this.name;
    }
}
