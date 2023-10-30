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
package ai.langstream.impl.noop;

import ai.langstream.api.model.StreamingCluster;
import ai.langstream.api.runner.topics.TopicConnectionsRuntime;
import ai.langstream.api.runner.topics.TopicConnectionsRuntimeProvider;
import ai.langstream.api.runner.topics.TopicConsumer;
import ai.langstream.api.runner.topics.TopicOffsetPosition;
import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.api.runner.topics.TopicReader;
import java.util.Map;

public class NoopTopicConnectionsRuntimeProvider implements TopicConnectionsRuntimeProvider {
    @Override
    public boolean supports(String streamingClusterType) {
        return "noop".equals(streamingClusterType);
    }

    @Override
    public TopicConnectionsRuntime getImplementation() {
        return NoopTopicConnectionsRuntime.INSTANCE;
    }

    private static class NoopTopicConnectionsRuntime implements TopicConnectionsRuntime {

        static final TopicConnectionsRuntime INSTANCE = new NoopTopicConnectionsRuntime();

        @Override
        public TopicConsumer createConsumer(
                String agentId,
                StreamingCluster streamingCluster,
                Map<String, Object> configuration) {
            throw new UnsupportedOperationException(
                    "You have not configured a streamingCluster for this application.");
        }

        @Override
        public TopicReader createReader(
                StreamingCluster streamingCluster,
                Map<String, Object> configuration,
                TopicOffsetPosition initialPosition) {
            throw new UnsupportedOperationException(
                    "You have not configured a streamingCluster for this application.");
        }

        @Override
        public TopicProducer createProducer(
                String agentId,
                StreamingCluster streamingCluster,
                Map<String, Object> configuration) {
            throw new UnsupportedOperationException(
                    "You have not configured a streamingCluster for this application.");
        }
    }
}
