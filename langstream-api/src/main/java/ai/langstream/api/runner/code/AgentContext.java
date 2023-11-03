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
package ai.langstream.api.runner.code;

import ai.langstream.api.runner.topics.TopicAdmin;
import ai.langstream.api.runner.topics.TopicConnectionProvider;
import ai.langstream.api.runner.topics.TopicConsumer;
import ai.langstream.api.runner.topics.TopicProducer;
import java.nio.file.Path;
import java.util.Optional;

public interface AgentContext {
    BadRecordHandler DEFAULT_BAD_RECORD_HANDLER =
            (record, t, cleanup) -> {
                cleanup.run();
                if (t instanceof RuntimeException) {
                    throw (RuntimeException) t;
                }
                throw new RuntimeException(t);
            };

    TopicConsumer getTopicConsumer();

    TopicProducer getTopicProducer();

    String getGlobalAgentId();

    TopicAdmin getTopicAdmin();

    TopicConnectionProvider getTopicConnectionProvider();

    default MetricsReporter getMetricsReporter() {
        return MetricsReporter.DISABLED;
    }

    default BadRecordHandler getBadRecordHandler() {
        return DEFAULT_BAD_RECORD_HANDLER;
    }

    default void criticalFailure(Throwable error) {}

    Path getCodeDirectory();

    /**
     * Access the persistent disk path for the agent. This is available only of the agent is
     * configured to have a persistent disk.
     *
     * @param agentId the agent id, this is important in case of a "composable" agent, that runs
     *     multiple agents in the same pod
     */
    default Optional<Path> getPersistentStateDirectoryForAgent(String agentId) {
        return Optional.empty();
    }
}
