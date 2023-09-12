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
package ai.langstream.api.runner.topics;

import ai.langstream.api.model.StreamingCluster;
import ai.langstream.api.runtime.ExecutionPlan;
import java.util.Map;

/** This is the interface that the LangStream runtime to connect to Topics. */
public interface TopicConnectionsRuntime {

    default void init(StreamingCluster streamingCluster) {}

    /**
     * Deploy the topics on the StreamingCluster
     *
     * @param applicationInstance the physical application instance
     */
    default void deploy(ExecutionPlan applicationInstance) {}

    /**
     * Undeploy all the resources created on the StreamingCluster
     *
     * @param applicationInstance the physical application instance
     */
    default void delete(ExecutionPlan applicationInstance) {}

    TopicConsumer createConsumer(
            String agentId, StreamingCluster streamingCluster, Map<String, Object> configuration);

    TopicReader createReader(
            StreamingCluster streamingCluster,
            Map<String, Object> configuration,
            TopicOffsetPosition initialPosition);

    TopicProducer createProducer(
            String agentId, StreamingCluster streamingCluster, Map<String, Object> configuration);

    default TopicProducer createDeadletterTopicProducer(
            String agentId, StreamingCluster streamingCluster, Map<String, Object> configuration) {
        return null;
    }

    default TopicAdmin createTopicAdmin(
            String agentId, StreamingCluster streamingCluster, Map<String, Object> configuration) {
        return new TopicAdmin() {};
    }

    default void close() {}
}
