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
package ai.langstream.pravega.runner;

import ai.langstream.api.model.Application;
import ai.langstream.api.model.StreamingCluster;
import ai.langstream.api.model.TopicDefinition;
import ai.langstream.api.runner.topics.TopicAdmin;
import ai.langstream.api.runner.topics.TopicConnectionsRuntime;
import ai.langstream.api.runner.topics.TopicConnectionsRuntimeProvider;
import ai.langstream.api.runner.topics.TopicConsumer;
import ai.langstream.api.runner.topics.TopicOffsetPosition;
import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.api.runner.topics.TopicReader;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.Topic;
import ai.langstream.pravega.PravegaClientUtils;
import ai.langstream.pravega.PravegaClusterRuntimeConfiguration;
import ai.langstream.pravega.PravegaTopic;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import java.util.HashMap;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PravegaTopicConnectionsRuntimeProvider implements TopicConnectionsRuntimeProvider {
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public boolean supports(String streamingClusterType) {
        return "pravega".equals(streamingClusterType);
    }

    @Override
    public TopicConnectionsRuntime getImplementation() {
        return new PravegaTopicConnectionsRuntime();
    }

    private static class PravegaTopicConnectionsRuntime implements TopicConnectionsRuntime {

        private EventStreamClientFactory client;

        @Override
        @SneakyThrows
        public void init(StreamingCluster streamingCluster) {
            client = PravegaClientUtils.buildPravegaClient(streamingCluster);
        }

        @Override
        @SneakyThrows
        public void close() {
            if (client != null) {
                client.close();
            }
        }

        @Override
        public TopicReader createReader(
                StreamingCluster streamingCluster,
                Map<String, Object> configuration,
                TopicOffsetPosition initialPosition) {
            Map<String, Object> copy = new HashMap<>(configuration);
            throw new UnsupportedOperationException();
        }

        @Override
        public TopicConsumer createConsumer(
                String agentId,
                StreamingCluster streamingCluster,
                Map<String, Object> configuration) {
            Map<String, Object> copy = new HashMap<>(configuration);
            copy.remove("deadLetterTopicProducer");
            throw new UnsupportedOperationException();
        }

        @Override
        public TopicProducer createProducer(
                String agentId,
                StreamingCluster streamingCluster,
                Map<String, Object> configuration) {
            Map<String, Object> copy = new HashMap<>(configuration);
            throw new UnsupportedOperationException();
        }

        @Override
        public TopicProducer createDeadletterTopicProducer(
                String agentId,
                StreamingCluster streamingCluster,
                Map<String, Object> configuration) {
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
                String agentId,
                StreamingCluster streamingCluster,
                Map<String, Object> configuration) {
            return new TopicAdmin() {};
        }

        @Override
        @SneakyThrows
        public void deploy(ExecutionPlan applicationInstance) {
            Application logicalInstance = applicationInstance.getApplication();
            PravegaClusterRuntimeConfiguration configuration =
                    PravegaClientUtils.getPravegarClusterRuntimeConfiguration(
                            logicalInstance.getInstance().streamingCluster());
            try (StreamManager admin = PravegaClientUtils.buildStreamManager(configuration)) {
                for (Topic topic : applicationInstance.getLogicalTopics()) {
                    deployTopic(admin, (PravegaTopic) topic);
                }
            }
        }

        private static void deployTopic(StreamManager admin, PravegaTopic topic) throws Exception {
            String createMode = topic.createMode();
            StreamConfiguration streamConfig =
                    StreamConfiguration.builder()
                            .scalingPolicy(ScalingPolicy.fixed(topic.partitions()))
                            .build();
            boolean exists = admin.checkStreamExists(topic.scope(), topic.name());
            switch (createMode) {
                case TopicDefinition.CREATE_MODE_CREATE_IF_NOT_EXISTS -> {
                    if (!exists) {
                        log.info("Topic {} does not exist, creating", topic.name());
                        admin.createStream(topic.scope(), topic.name(), streamConfig);
                    }
                }
                case TopicDefinition.CREATE_MODE_NONE -> {
                    // do nothing
                }
                default -> throw new IllegalArgumentException("Unknown create mode " + createMode);
            }
        }

        private static void deleteTopic(StreamManager admin, PravegaTopic topic) throws Exception {

            switch (topic.createMode()) {
                case TopicDefinition.CREATE_MODE_CREATE_IF_NOT_EXISTS -> {}
                default -> {
                    log.info(
                            "Keeping Pravega stream {} since creation-mode is {}",
                            topic.name(),
                            topic.createMode());
                    return;
                }
            }

            if (!topic.deleteMode().equals(TopicDefinition.DELETE_MODE_DELETE)) {
                log.info(
                        "Keeping Pravega stream {} since deletion-mode is {}",
                        topic.name(),
                        topic.deleteMode());
                return;
            }

            if (!admin.checkStreamExists(topic.scope(), topic.name())) {
                return;
            }

            log.info("Deleting topic {} in scope {}", topic.name(), topic.scope());
            try {
                admin.deleteStream(topic.scope(), topic.name());
            } catch (Exception error) {
                log.info("Topic {} didn't exit. Not a problem", topic);
            }
        }

        @Override
        @SneakyThrows
        public void delete(ExecutionPlan applicationInstance) {
            Application logicalInstance = applicationInstance.getApplication();
            try (StreamManager admin =
                    PravegaClientUtils.buildStreamManager(
                            logicalInstance.getInstance().streamingCluster())) {
                for (Topic topic : applicationInstance.getLogicalTopics()) {
                    deleteTopic(admin, (PravegaTopic) topic);
                }
            }
        }
    }
}
