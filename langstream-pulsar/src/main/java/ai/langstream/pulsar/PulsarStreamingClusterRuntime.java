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
package ai.langstream.pulsar;

import ai.langstream.api.model.SchemaDefinition;
import ai.langstream.api.model.StreamingCluster;
import ai.langstream.api.model.TopicDefinition;
import ai.langstream.api.runtime.AgentNode;
import ai.langstream.api.runtime.ConnectionImplementation;
import ai.langstream.api.runtime.StreamingClusterRuntime;
import ai.langstream.api.runtime.Topic;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PulsarStreamingClusterRuntime implements StreamingClusterRuntime {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public Topic createTopicImplementation(
            TopicDefinition topicDefinition, StreamingCluster streamingCluster) {
        final PulsarClusterRuntimeConfiguration config =
                getPulsarClusterRuntimeConfiguration(streamingCluster);

        SchemaDefinition keySchema = topicDefinition.getKeySchema();
        SchemaDefinition valueSchema = topicDefinition.getValueSchema();
        String name = topicDefinition.getName();
        String tenant = config.defaultTenant();
        String creationMode = topicDefinition.getCreationMode();
        String namespace = config.defaultNamespace();
        PulsarName topicName = new PulsarName(tenant, namespace, name);
        return new PulsarTopic(
                topicName,
                topicDefinition.getPartitions(),
                keySchema,
                valueSchema,
                creationMode,
                topicDefinition.getDeletionMode(),
                topicDefinition.isImplicit(),
                topicDefinition.getOptions());
    }

    @Override
    public Map<String, Object> createConsumerConfiguration(
            AgentNode agentImplementation, ConnectionImplementation inputConnectionImplementation) {
        PulsarTopic pulsarTopic = (PulsarTopic) inputConnectionImplementation;
        Map<String, Object> configuration = pulsarTopic.createConsumerConfiguration();
        configuration.computeIfAbsent(
                "subscriptionName", key -> "langstream-agent-" + agentImplementation.getId());
        return configuration;
    }

    @Override
    public Map<String, Object> createProducerConfiguration(
            AgentNode agentImplementation,
            ConnectionImplementation outputConnectionImplementation) {
        PulsarTopic pulsarTopic = (PulsarTopic) outputConnectionImplementation;
        return pulsarTopic.createProducerConfiguration();
    }

    public static PulsarClusterRuntimeConfiguration getPulsarClusterRuntimeConfiguration(
            StreamingCluster streamingCluster) {
        final Map<String, Object> configuration = streamingCluster.configuration();
        return MAPPER.convertValue(configuration, PulsarClusterRuntimeConfiguration.class);
    }
}
