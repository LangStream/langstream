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
package ai.langstream.pulsar;

import ai.langstream.api.model.SchemaDefinition;
import ai.langstream.api.runtime.ConnectionImplementation;
import ai.langstream.api.runtime.Topic;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public record PulsarTopic(
        PulsarName name,
        int partitions,
        SchemaDefinition keySchema,
        SchemaDefinition valueSchema,
        String createMode,
        String deleteMode,
        boolean implicit,
        Map<String, Object> options)
        implements ConnectionImplementation, Topic {

    public PulsarTopic {
        // options must be a mutable map, because we can dynamically add options
        // for instance the deadLetter configuration
        if (options == null) {
            options = new HashMap<>();
        } else {
            options = new HashMap<>(options);
        }
    }

    @Override
    public String topicName() {
        return name.toPulsarName();
    }

    @Override
    public boolean implicit() {
        return this.implicit;
    }

    public Map<String, Object> createConsumerConfiguration() {
        Map<String, Object> configuration = new HashMap<>();

        // this is for the Agent
        configuration.put("topic", name.toPulsarName());

        if (options != null) {
            options.forEach(
                    (key, value) -> {
                        if (key.startsWith("consumer.")) {
                            configuration.put(key.substring("consumer.".length()), value);
                        }
                    });

            Object deadLetterTopicProducer = options.get("deadLetterTopicProducer");
            if (deadLetterTopicProducer != null) {
                configuration.put("deadLetterTopicProducer", deadLetterTopicProducer);
            }
        }

        return configuration;
    }

    public Map<String, Object> createProducerConfiguration() {
        Map<String, Object> configuration = new HashMap<>();

        // this is for the Agent
        configuration.put("topic", name.toPulsarName());
        if (keySchema != null) {
            configuration.put("keySchema", keySchema);
        }
        if (valueSchema != null) {
            configuration.put("valueSchema", valueSchema);
        }

        if (options != null) {
            options.forEach(
                    (key, value) -> {
                        if (key.startsWith("producer.")) {
                            configuration.put(key.substring("producer.".length()), value);
                        }
                    });
        }

        return configuration;
    }

    @Override
    public void bindDeadletterTopic(Topic deadletterTopic) {
        if (!(deadletterTopic instanceof PulsarTopic pulsarTopic)) {
            throw new IllegalArgumentException();
        }
        log.info("Binding deadletter topic {} to topic {}", deadletterTopic, this.topicName());
        options.put("deadLetterTopicProducer", pulsarTopic.createProducerConfiguration());
    }
}
