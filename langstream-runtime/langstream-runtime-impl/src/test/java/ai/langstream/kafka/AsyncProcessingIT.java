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

import ai.langstream.AbstractApplicationRunner;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.Test;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Slf4j
class AsyncProcessingIT extends AbstractApplicationRunner  {

    @Test
    public void testProcessMultiThreadOutOfOrder() throws Exception {
        String tenant = "tenant";
        String[] expectedAgents = {"app-step1"};

        Map<String, String> application = Map.of(
                        "module.yaml", """
                                module: "module-1"
                                id: "pipeline-1"
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                    partitions: 4
                                  - name: "output-topic"
                                    creation-mode: create-if-not-exists
                                    partitions: 2
                                pipeline:
                                  - name: "async-process-records"
                                    id: "step1"
                                    type: "mock-async-processor"
                                    input: "input-topic"
                                    output: "output-topic"
                                """);

        try (ApplicationRuntime applicationRuntime = deployApplication(tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer();
                         KafkaConsumer<String, String> consumer = createConsumer("output-topic")) {

                Set<String> expected = new HashSet<>();
                int numMessages = 100;
                for (int i = 0; i < numMessages; i++) {
                    String content = "test message " + i;
                    expected.add(content);
                    sendMessage("input-topic", content, producer);
                }

                executeAgentRunners(applicationRuntime);

                waitForMessagesInAnyOrder(consumer, expected);
            }
        }

    }



}
