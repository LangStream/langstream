package com.dastastax.oss.sga.kafka.runner;

import com.dastastax.oss.sga.kafka.runtime.KafkaClusterRuntimeConfiguration;
import com.dastastax.oss.sga.kafka.runtime.KafkaStreamingClusterRuntime;
import com.datastax.oss.sga.api.model.StreamingCluster;
import com.datastax.oss.sga.api.runner.topics.TopicAdmin;
import com.datastax.oss.sga.api.runner.topics.TopicConnectionsRuntime;
import com.datastax.oss.sga.api.runner.topics.TopicConsumer;
import com.datastax.oss.sga.api.runner.topics.TopicProducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;

import java.util.HashMap;
import java.util.Map;

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
        copy.putIfAbsent("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        copy.putIfAbsent("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        copy.putIfAbsent("enable.auto.commit", "false");
        copy.putIfAbsent("group.id", "sga-" + agentId);
        copy.putIfAbsent("auto.offset.reset", "earliest");

        // producer
        copy.putIfAbsent("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        copy.putIfAbsent("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    }

    @Override
    public TopicProducer createProducer(String agentId, StreamingCluster streamingCluster, Map<String, Object> configuration) {
        Map<String, Object> copy = new HashMap<>(configuration);
        applyDefaultConfiguration(agentId, streamingCluster, copy);
        String topicName = (String) copy.remove("topic");

        return new KafkaProducerWrapper(copy, topicName);
    }

    @Override
    public TopicAdmin createTopicAdmin(String agentId, StreamingCluster streamingCluster, Map<String, Object> configuration) {
        Map<String, Object> copy = new HashMap<>(configuration);
        applyDefaultConfiguration(agentId, streamingCluster, copy);
        return new TopicAdmin() {

            org.apache.kafka.connect.util.TopicAdmin topicAdmin;
            @Override
            public void start() {
                topicAdmin = new org.apache.kafka.connect.util.TopicAdmin(copy);
            }

            @Override
            public void close() {
                if (topicAdmin != null) {
                    topicAdmin.close();
                    topicAdmin = null;
                }
            }

            @Override
            public Object getNativeTopicAdmin() {
                return topicAdmin;
            }
        };
    }

}
