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
import com.datastax.oss.sga.api.runner.code.AgentInfo;
import com.datastax.oss.sga.common.AbstractApplicationRunner;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.checkerframework.checker.units.qual.K;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Slf4j
class GenIAgentsRunnerTest extends AbstractApplicationRunner  {



    @Test
    public void testRunAITools() throws Exception {
        String tenant = "tenant";
        String[] expectedAgents = {"app-step1"};

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
                                  - name: "drop-description"
                                    id: "step1"
                                    type: "drop-fields"
                                    input: "input-topic"
                                    output: "output-topic"
                                    configuration:
                                      fields:
                                        - "description"
                                """);

        try (ApplicationRuntime applicationRuntime = deployApplication(tenant, "app", application, expectedAgents)) {


        try (KafkaProducer<String, String> producer = createProducer();
                     KafkaConsumer<String, String> consumer = createConsumer("output-topic")) {


            sendMessage("input-topic","{\"name\": \"some name\", \"description\": \"some description\"}",
                    List.of(new RecordHeader("header-key", "header-value".getBytes(StandardCharsets.UTF_8))),
                    producer);

            executeAgentRunners(applicationRuntime);

            List<ConsumerRecord> records = waitForMessages(consumer, List.of("{\"name\":\"some name\"}"));

            ConsumerRecord<String, String> record = records.get(0);
            assertEquals("{\"name\":\"some name\"}", record.value());
            assertEquals("header-value", new String(record.headers().lastHeader("header-key").value(), StandardCharsets.UTF_8));
        }
        }

    }


    @Test
    public void testRunAIToolsComposite() throws Exception {
        String tenant = "tenant";
        String[] expectedAgents = {"app-step1"};

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
                                  - name: "drop-description"
                                    id: "step1"
                                    type: "drop-fields"
                                    input: "input-topic"
                                    configuration:
                                      fields:
                                        - "description"
                                  - name: "drop"
                                    id: "step2"
                                    type: "drop"
                                    output: "output-topic"
                                """);

        try (ApplicationRuntime applicationRuntime = deployApplication(tenant, "app", application, expectedAgents)) {

            final AgentRunResult result = executeAgentRunners(applicationRuntime);

            final Map<String, Object> infos = result.info().get("step1")
                    .serveInfos();
            System.out.println("result: " + infos);
            final AgentInfo processor = (AgentInfo) infos.get("processor");
            final Map<String, Object> compositeInfo = (Map<String, Object>) processor.getInfo();
            final List<Map<String, Object>> processors = (List<Map<String, Object>>) compositeInfo.get("processors");
            assertEquals(2, processors.size());
            for (Map<String, Object> p : processors) {
                switch (String.valueOf(p.get("agent-id"))) {
                    case "step1":
                        assertEquals("drop-fields", p.get("agent-type"));
                        break;
                    case "step2":
                        assertEquals("drop", p.get("agent-type"));
                        break;
                    default:
                        throw new IllegalStateException("Unexpected value: " + p.get("agent-id"));
                }
                assertEquals(0L, p.get("total-in"));
                assertEquals(0L, p.get("total-out"));
                assertNotEquals(0L, p.get("started-at"));
                assertEquals(0L, p.get("last-processed-at"));
            }

        }

    }


}
