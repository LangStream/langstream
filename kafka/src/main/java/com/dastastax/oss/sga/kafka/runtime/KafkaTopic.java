package com.dastastax.oss.sga.kafka.runtime;

import com.datastax.oss.sga.api.runtime.ConnectionImplementation;
import com.datastax.oss.sga.api.runtime.TopicImplementation;

import java.util.HashMap;
import java.util.Map;

public record KafkaTopic(String name, String schemaName, String schemaType, String schema, String createMode)
        implements ConnectionImplementation, TopicImplementation {
    public Map<String,Object> createConsumerConfiguration() {
        Map<String, Object> configuration = new HashMap<>();

        // default
        configuration.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        configuration.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // TODO: handle schema

        return configuration;
    }

    public Map<String,Object> createProducerConfiguration() {
        Map<String, Object> configuration = new HashMap<>();

        // default
        configuration.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        configuration.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // TODO: handle schema

        return configuration;
    }
}
