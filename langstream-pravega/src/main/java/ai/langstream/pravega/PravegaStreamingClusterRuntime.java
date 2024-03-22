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
package ai.langstream.pravega;

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
public class PravegaStreamingClusterRuntime implements StreamingClusterRuntime {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public Topic createTopicImplementation(
            TopicDefinition topicDefinition, StreamingCluster streamingCluster) {
        final PravegaClusterRuntimeConfiguration config =
                getPravegaClusterRuntimeConfiguration(streamingCluster);

        String name = topicDefinition.getName();
        String creationMode = topicDefinition.getCreationMode();
        return new PravegaTopic(
                name,
                topicDefinition.getPartitions(),
                creationMode,
                topicDefinition.getDeletionMode(),
                topicDefinition.isImplicit(),
                topicDefinition.getOptions());
    }

    @Override
    public Map<String, Object> createConsumerConfiguration(
            AgentNode agentImplementation, ConnectionImplementation inputConnectionImplementation) {
        PravegaTopic pravegaTopic = (PravegaTopic) inputConnectionImplementation;
        Map<String, Object> configuration = pravegaTopic.createConsumerConfiguration();
        configuration.computeIfAbsent(
                "reader-group", key -> "langstream-agent-" + agentImplementation.getId());
        return configuration;
    }

    @Override
    public Map<String, Object> createProducerConfiguration(
            AgentNode agentImplementation,
            ConnectionImplementation outputConnectionImplementation) {
        PravegaTopic pravegaTopic = (PravegaTopic) outputConnectionImplementation;
        return pravegaTopic.createProducerConfiguration();
    }

    public static PravegaClusterRuntimeConfiguration getPravegaClusterRuntimeConfiguration(
            StreamingCluster streamingCluster) {
        final Map<String, Object> configuration = streamingCluster.configuration();
        return MAPPER.convertValue(configuration, PravegaClusterRuntimeConfiguration.class);
    }
}
