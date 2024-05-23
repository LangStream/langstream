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
package ai.langstream.kafka.runtime;

import ai.langstream.api.model.StreamingCluster;
import ai.langstream.api.model.TopicDefinition;
import ai.langstream.api.runtime.AgentNode;
import ai.langstream.api.runtime.ConnectionImplementation;
import ai.langstream.api.runtime.StreamingClusterRuntime;
import ai.langstream.api.runtime.Topic;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaStreamingClusterRuntime implements StreamingClusterRuntime {

    static final ObjectMapper mapper = new ObjectMapper();

    public static KafkaClusterRuntimeConfiguration getKafkaClusterRuntimeConfiguration(
            StreamingCluster streamingCluster) {
        final Map<String, Object> configuration = streamingCluster.configuration();
        return mapper.convertValue(configuration, KafkaClusterRuntimeConfiguration.class);
    }

    @Override
    public Topic createTopicImplementation(
            TopicDefinition topicDefinition, StreamingCluster streamingCluster) {
        String name = topicDefinition.getName();
        String creationMode = topicDefinition.getCreationMode();
        Map<String, Object> options = topicDefinition.getOptions();
        if (options == null) {
            options = new HashMap<>();
        }
        int replicationFactor =
                Integer.parseInt(options.getOrDefault("replication-factor", "1").toString());
        Map<String, Object> configs = topicDefinition.getConfig();
        return new KafkaTopic(
                name,
                topicDefinition.getPartitions() <= 0 ? 1 : topicDefinition.getPartitions(),
                replicationFactor,
                topicDefinition.getKeySchema(),
                topicDefinition.getValueSchema(),
                creationMode,
                topicDefinition.getDeletionMode(),
                topicDefinition.isImplicit(),
                configs,
                options);
    }

    @Override
    public Map<String, Object> createConsumerConfiguration(
            AgentNode agentImplementation, ConnectionImplementation inputConnectionImplementation) {
        KafkaTopic kafkaTopic = (KafkaTopic) inputConnectionImplementation;

        // handle schema
        Map<String, Object> configuration = new HashMap<>(kafkaTopic.createConsumerConfiguration());

        // TODO: handle other configurations
        configuration.computeIfAbsent(
                "group.id", key -> "langstream-agent-" + agentImplementation.getId());
        configuration.putIfAbsent("auto.offset.reset", "earliest");
        return configuration;
    }

    @Override
    public Map<String, Object> createProducerConfiguration(
            AgentNode agentImplementation,
            ConnectionImplementation outputConnectionImplementation) {
        KafkaTopic kafkaTopic = (KafkaTopic) outputConnectionImplementation;

        // handle schema
        // TODO: handle other configurations
        return new HashMap<>(kafkaTopic.createProducerConfiguration());
    }
}
