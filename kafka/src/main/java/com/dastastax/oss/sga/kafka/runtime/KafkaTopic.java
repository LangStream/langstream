package com.dastastax.oss.sga.kafka.runtime;

import com.datastax.oss.sga.api.runtime.Connection;
import com.datastax.oss.sga.api.runtime.Topic;

import java.util.HashMap;
import java.util.Map;

public record KafkaTopic(String name, String schemaName, String schemaType, String schema, String createMode)
        implements Connection, Topic {
    public Map<String,Object> createConsumerConfiguration() {
        Map<String, Object> configuration = new HashMap<>();


        // this is for the Agent
        configuration.put("topic", name);

        // default
        configuration.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        configuration.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // TODO: handle schema

        return configuration;
    }

    public Map<String,Object> createProducerConfiguration() {
        Map<String, Object> configuration = new HashMap<>();

        // this is for the Agent
        configuration.put("topic", name);

        // default
        configuration.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        configuration.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // TODO: handle schema

        return configuration;
    }
}
