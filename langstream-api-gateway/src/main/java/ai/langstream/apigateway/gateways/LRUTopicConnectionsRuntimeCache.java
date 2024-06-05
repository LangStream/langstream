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
package ai.langstream.apigateway.gateways;

import ai.langstream.api.model.StreamingCluster;
import ai.langstream.api.runner.topics.*;
import ai.langstream.api.runtime.ExecutionPlan;
import java.util.Map;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LRUTopicConnectionsRuntimeCache
        extends LRUCache<
                TopicConnectionsRuntimeCache.Key,
                LRUTopicConnectionsRuntimeCache.SharedTopicConnectionsRuntime>
        implements TopicConnectionsRuntimeCache {

    public static class SharedTopicConnectionsRuntime extends CachedObject<TopicConnectionsRuntime>
            implements TopicConnectionsRuntime {

        public SharedTopicConnectionsRuntime(TopicConnectionsRuntime topicConnectionsRuntime) {
            super(topicConnectionsRuntime);
        }

        @Override
        public void init(StreamingCluster streamingCluster) {
            object.init(streamingCluster);
        }

        @Override
        public void deploy(ExecutionPlan applicationInstance) {
            object.deploy(applicationInstance);
        }

        @Override
        public void delete(ExecutionPlan applicationInstance) {
            object.delete(applicationInstance);
        }

        @Override
        public TopicConsumer createConsumer(
                String agentId,
                StreamingCluster streamingCluster,
                Map<String, Object> configuration) {
            return object.createConsumer(agentId, streamingCluster, configuration);
        }

        @Override
        public TopicReader createReader(
                StreamingCluster streamingCluster,
                Map<String, Object> configuration,
                TopicOffsetPosition initialPosition) {
            return object.createReader(streamingCluster, configuration, initialPosition);
        }

        @Override
        public TopicProducer createProducer(
                String agentId,
                StreamingCluster streamingCluster,
                Map<String, Object> configuration) {
            return object.createProducer(agentId, streamingCluster, configuration);
        }

        @Override
        public TopicProducer createDeadletterTopicProducer(
                String agentId,
                StreamingCluster streamingCluster,
                Map<String, Object> configuration) {
            return object.createDeadletterTopicProducer(agentId, streamingCluster, configuration);
        }

        @Override
        public TopicAdmin createTopicAdmin(
                String agentId,
                StreamingCluster streamingCluster,
                Map<String, Object> configuration) {
            return object.createTopicAdmin(agentId, streamingCluster, configuration);
        }
    }

    public LRUTopicConnectionsRuntimeCache(int size) {
        super(size);
    }

    @Override
    public SharedTopicConnectionsRuntime getOrCreate(
            Key key, Supplier<TopicConnectionsRuntime> topicProducerSupplier) {
        return super.getOrCreate(
                key, () -> new SharedTopicConnectionsRuntime(topicProducerSupplier.get()));
    }
}
