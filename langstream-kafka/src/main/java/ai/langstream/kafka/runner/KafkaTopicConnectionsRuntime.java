/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.langstream.kafka.runner;

import ai.langstream.api.model.StreamingCluster;
import ai.langstream.api.runner.topics.TopicAdmin;
import ai.langstream.api.runner.topics.TopicConnectionsRuntime;
import ai.langstream.api.runner.topics.TopicConsumer;
import ai.langstream.api.runner.topics.TopicOffsetPosition;
import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.api.runner.topics.TopicReader;
import ai.langstream.kafka.runtime.KafkaClusterRuntimeConfiguration;
import ai.langstream.kafka.runtime.KafkaStreamingClusterRuntime;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaTopicConnectionsRuntime implements TopicConnectionsRuntime {

    @Override
    public TopicReader createReader(
            StreamingCluster streamingCluster,
            Map<String, Object> configuration,
            TopicOffsetPosition initialPosition) {
        Map<String, Object> copy = new HashMap<>(configuration);
        copy.putAll(
                KafkaStreamingClusterRuntime.getKafkaClusterRuntimeConfiguration(streamingCluster)
                        .getAdmin());
        copy.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        copy.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // do not use group id for reader. "group.id" default value is null, which is not accepted
        // by KafkaConsumer.
        copy.put("group.id", "");
        // only read one record at the time to have consistent offsets.
        copy.put("max.poll.records", 1);
        String topicName = (String) copy.remove("topic");
        return new KafkaReaderWrapper(copy, topicName, initialPosition);
    }

    @Override
    public TopicConsumer createConsumer(
            String agentId, StreamingCluster streamingCluster, Map<String, Object> configuration) {

        Map<String, Object> copy = new HashMap<>(configuration);
        applyDefaultConfiguration(streamingCluster, copy);
        applyConsumerConfiguration(agentId, copy);
        String topicName = (String) copy.remove("topic");

        return new KafkaConsumerWrapper(copy, topicName);
    }

    private void applyConsumerConfiguration(String agentId, Map<String, Object> copy) {
        copy.putIfAbsent(
                "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        copy.putIfAbsent(
                "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        copy.putIfAbsent("enable.auto.commit", "false");
        copy.putIfAbsent("group.id", "langstream-" + agentId);
        copy.putIfAbsent("auto.offset.reset", "earliest");
    }

    private static void applyDefaultConfiguration(StreamingCluster streamingCluster, Map<String, Object> copy) {
        KafkaClusterRuntimeConfiguration configuration =
                KafkaStreamingClusterRuntime.getKafkaClusterRuntimeConfiguration(streamingCluster);
        copy.putAll(configuration.getAdmin());
    }

    private static void applyProducerConfiguration(Map<String, Object> copy) {
        copy.putIfAbsent(
                "key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        copy.putIfAbsent(
                "value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    }

    @Override
    public TopicProducer createProducer(
            String agentId, StreamingCluster streamingCluster, Map<String, Object> configuration) {
        Map<String, Object> copy = new HashMap<>(configuration);
        applyDefaultConfiguration(streamingCluster, copy);
        applyProducerConfiguration(copy);
        String topicName = (String) copy.remove("topic");

        return new KafkaProducerWrapper(copy, topicName);
    }

    @Override
    public TopicProducer createDeadletterTopicProducer(
            String agentId, StreamingCluster streamingCluster, Map<String, Object> configuration) {
        Map<String, Object> deadletterConfiguration =
                (Map<String, Object>) configuration.get("deadLetterTopicProducer");
        if (deadletterConfiguration == null || deadletterConfiguration.isEmpty()) {
            return null;
        }
        log.info(
                "Creating deadletter topic producer for agent {} using configuration {}",
                agentId,
                configuration);
        return createProducer(agentId, streamingCluster, deadletterConfiguration);
    }

    @Override
    public TopicAdmin createTopicAdmin(
            String agentId, StreamingCluster streamingCluster, Map<String, Object> configuration) {
        Map<String, Object> copy = new HashMap<>(configuration);
        applyDefaultConfiguration(streamingCluster, copy);
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
