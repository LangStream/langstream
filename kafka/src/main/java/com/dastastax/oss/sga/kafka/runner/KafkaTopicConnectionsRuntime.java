package com.dastastax.oss.sga.kafka.runner;

import com.dastastax.oss.sga.kafka.runtime.KafkaClusterRuntimeConfiguration;
import com.dastastax.oss.sga.kafka.runtime.KafkaStreamingClusterRuntime;
import com.datastax.oss.sga.api.model.StreamingCluster;
import com.datastax.oss.sga.api.runner.topics.TopicConnectionsRuntime;
import com.datastax.oss.sga.api.runner.topics.TopicConsumer;
import com.datastax.oss.sga.api.runner.topics.TopicProducer;
import java.util.UUID;

import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.BooleanSerializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.ShortSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.UUIDSerializer;

@Slf4j
public class KafkaTopicConnectionsRuntime implements TopicConnectionsRuntime {


    @Override
    public TopicConsumer createConsumer(String agentId, StreamingCluster streamingCluster, Map<String, Object> configuration) {

        Map<String, Object> copy = new HashMap<>(configuration);
        applyDefaultConfiguration(agentId, streamingCluster, copy);
        String topicName = (String) copy.remove("topic");

        return new KafkaConsumerWrapper(copy, topicName);
    }

    private static void applyDefaultConfiguration(String agentId, StreamingCluster streamingCluster, Map<String, Object> copy) {
        KafkaClusterRuntimeConfiguration configuration = KafkaStreamingClusterRuntime.getKafkaClusterRuntimeConfiguration(streamingCluster);
        copy.putAll(configuration.getAdmin());

        // consumer
        copy.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        copy.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        copy.put("enable.auto.commit", "false");
        copy.computeIfAbsent("group.id", key -> "sga-" + agentId);
        copy.putIfAbsent("auto.offset.reset", "earliest");

        // producer
        copy.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        copy.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    }

    @Override
    public TopicProducer createProducer(String agentId, StreamingCluster streamingCluster, Map<String, Object> configuration) {
        Map<String, Object> copy = new HashMap<>(configuration);
        applyDefaultConfiguration(agentId, streamingCluster, copy);
        String topicName = (String) copy.remove("topic");

        return new KafkaProducerWrapper(copy, topicName);
    }

}
