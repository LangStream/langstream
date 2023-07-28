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
        String[] expectedAgents = {"app-text-extractor1"};

        Map<String, String> application = Map.of("instance.yaml",
                        buildInstanceYaml(),
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

        try (ApplicationRuntime applicationRuntime = deployApplication(tenant, "app", application, expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer();
                 KafkaConsumer<String, String> consumer = createConsumer("output-topic")) {

                sendMessage("input-topic","Questo testo è scritto in Italiano.", producer);
                sendMessage("input-topic","This text is written in English, but it is very long,\nso you may want to split it into chunks.", producer);

                executeAgentRunners(applicationRuntime);
                waitForMessages(consumer, List.of("this   text   is   written   in   english,   but",
                        "it   is   very   long,",
                        "so you may want to split it into chunks."));
            }
        }

    }

}