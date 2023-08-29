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

import ai.langstream.api.runtime.StreamingClusterRuntime;
import ai.langstream.api.runtime.StreamingClusterRuntimeProvider;
import ai.langstream.api.runtime.Topic;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NoOpStreamingClusterRuntimeProvider implements StreamingClusterRuntimeProvider {
    @Override
    public boolean supports(String type) {
        return "noop".equals(type);
    }

    @EqualsAndHashCode
    @ToString
    public static class SimpleTopic implements Topic {
        private final String name;
        private final boolean implicit;
        private Topic deadletterTopic;

        public SimpleTopic(String name, boolean implicit) {
            this.name = name;
            this.implicit = implicit;
        }

        @Override
        public String topicName() {
            return name;
        }

        @Override
        public boolean implicit() {
            return this.implicit;
        }

        @Override
        public void bindDeadletterTopic(Topic deadletterTopic) {
            log.error(
                    "Setting deadletter topic configuration on dummy cluster: {}", deadletterTopic);
            this.deadletterTopic = deadletterTopic;
        }

        public Topic getDeadletterTopic() {
            return deadletterTopic;
        }
    }

    @Override
    public StreamingClusterRuntime getImplementation() {
        return (topicDefinition, applicationInstance) ->
                new SimpleTopic(topicDefinition.getName(), topicDefinition.isImplicit());
    }
}
