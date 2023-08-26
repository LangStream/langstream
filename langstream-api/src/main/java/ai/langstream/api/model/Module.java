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
package ai.langstream.api.model;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Module {
    public static final String DEFAULT_MODULE = "default";
    private String id;

    private final Map<String, Pipeline> pipelines = new HashMap<>();

    private final Map<String, TopicDefinition> topics = new HashMap<>();

    public Module(String id) {
        this.id = id;
    }

    public Pipeline addPipeline(String pipelineId) {
        if (pipelines.containsKey(pipelineId)) {
            throw new IllegalArgumentException("Pipeline " + pipelineId + " already exists in module " + id);
        }
        Pipeline p = new Pipeline(pipelineId, id);
        pipelines.put(pipelineId, p);
        return p;
    }

    public TopicDefinition addTopic(TopicDefinition topicDefinition) {
        final String topicName = topicDefinition.getName();
        TopicDefinition existing = topics.get(topicName);
        if (existing != null) {
            // allow to declare the same topic in multiple pipelines of the same module
            // but only if the definition is the same
            if (!existing.equals(topicDefinition)) {
                throw new IllegalArgumentException("Pipeline " + topicName + " already exists in module " + id);
            }
            return existing;
        }
        topics.put(topicName, topicDefinition);
        return topicDefinition;
    }

    public TopicDefinition resolveTopic(String input) {
        TopicDefinition topicDefinition = topics.get(input);
        if (topicDefinition == null) {
            throw new IllegalArgumentException("Topic " + input + " is not defined in module " + id + ", only " + topics.keySet());
        }
        return topicDefinition;
    }
}
