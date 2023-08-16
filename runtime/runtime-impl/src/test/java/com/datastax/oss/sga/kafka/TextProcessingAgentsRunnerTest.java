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
package com.datastax.oss.sga.kafka;

import com.datastax.oss.sga.common.AbstractApplicationRunner;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;

@Slf4j
class TextProcessingAgentsRunnerTest extends AbstractApplicationRunner {

    @Test
    public void testFullLanguageProcessingPipeline() throws Exception {
        String tenant = "tenant";
        String[] expectedAgents = {"app-module-1-pipeline-1-text-extractor-1"};

        Map<String, String> application = Map.of(
                        "module.yaml", """
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
                                        makeLowercase: true
                                        trimSpaces: true
                                """);

        try (ApplicationRuntime applicationRuntime = deployApplication(tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer();
                 KafkaConsumer<String, String> consumer = createConsumer("output-topic")) {

                sendMessage("input-topic","Questo testo è scritto in Italiano.", producer);
                sendMessage("input-topic","This text is written in English, but it is very long,\nso you may want to split it into chunks.", producer);

                executeAgentRunners(applicationRuntime);
                waitForMessages(consumer, List.of("this text is written in english, but it is very", "long,", "so you may want to split it into chunks."));
            }
        }

    }

}
