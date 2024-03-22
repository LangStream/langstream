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
package ai.langstream.api.runtime;

import ai.langstream.api.model.StreamingCluster;
import ai.langstream.api.model.TopicDefinition;
import java.util.Map;

/**
 * This is the interface that the LangStream framework uses to interact with the StreamingCluster.
 * It is used to model a physical cluster runtime with Brokers (Pulsar, Kafka....)
 */
public interface StreamingClusterRuntime extends AutoCloseable {

    /**
     * Map a Logical TopicDefinition to a Physical TopicImplementation
     *
     * @param topicDefinition the logical topic definition
     * @param streamingCluster streaming cluster configuration
     * @return the physical topic implementation
     */
    Topic createTopicImplementation(
            TopicDefinition topicDefinition, StreamingCluster streamingCluster);

    /**
     * Create the configuration to consume from a topic. The contents of the map are specific to the
     * StreamingCluster implementation.
     *
     * @param inputConnectionImplementation the input connection implementation
     * @return the configuration
     */
    default Map<String, Object> createConsumerConfiguration(
            AgentNode agentImplementation, ConnectionImplementation inputConnectionImplementation) {
        return Map.of();
    }

    /**
     * Create the configuration to produce to a topic. The contents of the map are specific to the
     * StreamingCluster implementation.
     *
     * @param outputConnectionImplementation the output connection implementation
     * @return the configuration
     */
    default Map<String, Object> createProducerConfiguration(
            AgentNode agentImplementation,
            ConnectionImplementation outputConnectionImplementation) {
        return Map.of();
    }

    default void close() {}
}
