/**
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
import ai.langstream.api.runner.topics.TopicConsumer;
import ai.langstream.api.runner.topics.TopicConnectionProvider;
import ai.langstream.api.runner.topics.TopicProducer;

public interface AgentContext {
    BadRecordHandler DEFAULT_BAD_RECORD_HANDLER = new BadRecordHandler() {
        @Override
        public void handle(Record record, Throwable t, Runnable cleanup) throws RuntimeException {
            cleanup.run();
            if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            }
            throw new RuntimeException(t);
        }
    };

    TopicConsumer getTopicConsumer();

    TopicProducer getTopicProducer();

    String getAgentId();

    TopicAdmin getTopicAdmin();

    TopicConnectionProvider getTopicConnectionProvider();

    default BadRecordHandler getBadRecordHandler() {
        return DEFAULT_BAD_RECORD_HANDLER;
    }
}
