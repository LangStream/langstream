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
package com.datastax.oss.sga.impl.noop;

import com.datastax.oss.sga.api.model.TopicDefinition;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.StreamingClusterRuntime;
import com.datastax.oss.sga.api.runtime.StreamingClusterRuntimeProvider;
import com.datastax.oss.sga.api.runtime.Topic;

public class NoOpStreamingClusterRuntimeProvider implements StreamingClusterRuntimeProvider {
    @Override
    public boolean supports(String type) {
        return "noop".equals(type);
    }

    public record SimpleTopic(String name, boolean implicit) implements Topic {
        @Override
        public String topicName() {
            return name;
        }

        @Override
        public boolean implicit() {
            return this.implicit;
        }
    }
    @Override
    public StreamingClusterRuntime getImplementation() {
        return new StreamingClusterRuntime() {
            @Override
            public Topic createTopicImplementation(TopicDefinition topicDefinition, ExecutionPlan applicationInstance) {
                return new SimpleTopic(topicDefinition.getName(), topicDefinition.isImplicit());
            }
        };
    }
}
