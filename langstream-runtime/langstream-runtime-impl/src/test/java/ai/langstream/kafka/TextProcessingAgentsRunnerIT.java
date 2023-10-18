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
package ai.langstream.kafka;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Slf4j
class TextProcessingAgentsRunnerIT extends AbstractKafkaApplicationRunner {

    @Test
    public void testFullLanguageProcessingPipeline() throws Exception {
        String tenant = "tenant";
        String[] expectedAgents = {"app-module-1-pipeline-1-text-extractor-1"};

        Map<String, String> application =
                Map.of(
                        "module.yaml",
                        """
                                module: "module-1"
                                id: "pipeline-1"
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                  - name: "output-topic"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "Extract text"
                                    type: "text-extractor"
                                    input: "input-topic"
                                  - name: "Detect language"
                                    type: "language-detector"
                                    configuration:
                                       allowedLanguages: ["en"]
                                       property: "language"
                                  - name: "Split into chunks"
                                    type: "text-splitter"
                                    configuration:
                                      chunk_size: 50
                                      chunk_overlap: 0
                                      keep_separator: true
                                      length_function: "length"
                                  - name: "Normalise text"
                                    type: "text-normaliser"
                                    output: "output-topic"
                                    configuration:
                                        make-lowercase: true
                                        trim-spaces: true
                                """);

        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer();
                    KafkaConsumer<String, String> consumer = createConsumer("output-topic")) {

                sendMessage("input-topic", "Questo testo è scritto in Italiano.", producer);
                sendMessage(
                        "input-topic",
                        "This text is written in English, but it is very long,\nso you may want to split it into chunks.",
                        producer);

                executeAgentRunners(applicationRuntime);
                waitForMessages(
                        consumer,
                        List.of(
                                "this text is written in english, but it is very",
                                "long,",
                                "so you may want to split it into chunks."));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testSplitThenJson(boolean useIntermediateTopic) throws Exception {
        String tenant = "tenant";
        String[] expectedAgents =
                useIntermediateTopic
                        ? new String[] {"app-step1", "app-step2"}
                        : new String[] {"app-step1"};
        String inputTopic = "input-topic-" + UUID.randomUUID();
        String outputTopic = "output-topic-" + UUID.randomUUID();
        String intermediateTopic = "intermediate-topic-" + UUID.randomUUID();

        Map<String, String> application =
                Map.of(
                        "module.yaml",
                        useIntermediateTopic
                                ? """
                                module: "module-1"
                                id: "pipeline-1"
                                topics:
                                  - name: "%s"
                                    creation-mode: create-if-not-exists
                                  - name: "%s"
                                    creation-mode: create-if-not-exists
                                  - name: "%s"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "Split into chunks"
                                    id: step1
                                    type: "text-splitter"
                                    input: %s
                                    output: %s
                                    configuration:
                                      chunk_size: 50
                                      chunk_overlap: 0
                                      keep_separator: true
                                      length_function: "length"
                                  - name: "Convert chunks to JSON"
                                    type: "document-to-json"
                                    id: step2
                                    input: %s
                                    output: %s
                                    configuration:
                                        text-field: text
                                        copy-properties: true
                                """
                                        .formatted(
                                                inputTopic,
                                                outputTopic,
                                                intermediateTopic,
                                                inputTopic,
                                                intermediateTopic,
                                                intermediateTopic,
                                                outputTopic)
                                : """
                                module: "module-1"
                                id: "pipeline-1"
                                topics:
                                  - name: "%s"
                                    creation-mode: create-if-not-exists
                                  - name: "%s"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "Split into chunks"
                                    id: step1
                                    type: "text-splitter"
                                    input: %s
                                    configuration:
                                      chunk_size: 50
                                      chunk_overlap: 0
                                      keep_separator: true
                                      length_function: "length"
                                  - name: "Convert chunks to JSON"
                                    type: "document-to-json"
                                    output: %s
                                    configuration:
                                        text-field: text
                                        copy-properties: true
                                """
                                        .formatted(
                                                inputTopic, outputTopic, inputTopic, outputTopic));

        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer();
                    KafkaConsumer<String, String> consumer = createConsumer(outputTopic)) {

                sendMessage(
                        inputTopic,
                        "This text is written in English, but it is very long,\nso you may want to split it into chunks.",
                        producer);

                executeAgentRunners(applicationRuntime);
                waitForMessages(
                        consumer,
                        List.of(
                                "{\"chunk_text_length\":\"47\",\"text\":\"This text is written in English, but it is very\",\"text_num_chunks\":\"3\",\"chunk_id\":\"0\",\"chunk_num_tokens\":\"47\"}",
                                "{\"chunk_text_length\":\"5\",\"text\":\"long,\",\"text_num_chunks\":\"3\",\"chunk_id\":\"1\",\"chunk_num_tokens\":\"5\"}",
                                "{\"chunk_text_length\":\"40\",\"text\":\"so you may want to split it into chunks.\",\"text_num_chunks\":\"3\",\"chunk_id\":\"2\",\"chunk_num_tokens\":\"40\"}"));
            }
        }
    }
}
