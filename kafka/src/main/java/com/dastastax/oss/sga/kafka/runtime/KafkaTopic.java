package com.dastastax.oss.sga.kafka.runtime;

import com.datastax.oss.sga.api.model.SchemaDefinition;
import com.datastax.oss.sga.api.runtime.Connection;
import com.datastax.oss.sga.api.runtime.Topic;

import java.util.HashMap;
import java.util.Map;

public record KafkaTopic(String name, int partitions, int replicationFactor, SchemaDefinition keySchema, SchemaDefinition valueSchema, String createMode,
                         boolean implicit,
                         Map<String, Object> config, Map<String, Object> options)
        implements Connection, Topic {
    public Map<String,Object> createConsumerConfiguration() {
        Map<String, Object> configuration = new HashMap<>();


        // this is for the Agent
        configuration.put("topic", name);

        if (options != null) {
            options.forEach((key, value) -> {
                if (key.startsWith("consumer.")) {
                    configuration.put(key.substring("consumer.".length()), value);
                }
            });
        }

        // TODO: handle schema

        return configuration;
    }

    public Map<String,Object> createProducerConfiguration() {
        Map<String, Object> configuration = new HashMap<>();

        // this is for the Agent
        configuration.put("topic", name);

        if (options != null) {
            options.forEach((key, value) -> {
                if (key.startsWith("producer.")) {
                    configuration.put(key.substring("producer.".length()), value);
                }
            });
        }

        // TODO: handle schema

        return configuration;
    }

    @Override
    public String topicName() {
        return this.name;
    }

    @Override
    public boolean implicit() {
        return this.implicit;
    }
}
