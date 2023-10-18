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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.junit.jupiter.api.Test;

@Slf4j
class KafkaConnectSourceRunnerIT extends AbstractKafkaApplicationRunner {

    @Test
    public void testRunKafkaConnectSource() throws Exception {
        String tenant = "tenant";
        String[] expectedAgents = {"app-step1"};

        Map<String, String> application =
                Map.of(
                        "module.yaml",
                        """
                                module: "module-1"
                                id: "pipeline-1"
                                topics:
                                  - name: "output-topic"
                                    creation-mode: create-if-not-exists
                                  - name: "offset-topic"
                                    creation-mode: create-if-not-exists
                                    partitions: 1
                                    options:
                                      replication-factor: 1
                                    config:
                                      cleanup.policy: compact
                                pipeline:
                                  - name: "source1"
                                    id: "step1"
                                    type: "source"
                                    output: "output-topic"
                                    configuration:
                                      connector.class: %s
                                      num-messages: 5
                                      offset.storage.topic: "offset-topic"
                                """
                                .formatted(DummySourceConnector.class.getName()));

        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            try (KafkaConsumer<String, String> consumer = createConsumer("output-topic")) {

                executeAgentRunners(applicationRuntime);
                waitForMessages(
                        consumer,
                        List.of("message-0", "message-1", "message-2", "message-3", "message-4"));
            }
        }
    }

    public static final class DummySourceConnector extends SourceConnector {

        private Map<String, String> connectorConfiguration;

        @Override
        public void start(Map<String, String> map) {
            this.connectorConfiguration = map;
        }

        @Override
        public Class<? extends Task> taskClass() {
            return DummySource.class;
        }

        @Override
        public List<Map<String, String>> taskConfigs(int i) {
            // we pass the whole connector configuration to the task
            return List.of(connectorConfiguration);
        }

        @Override
        public void stop() {}

        @Override
        public ConfigDef config() {
            return new ConfigDef();
        }

        @Override
        public String version() {
            return "1.0";
        }
    }

    public static final class DummySource extends SourceTask {

        BlockingQueue<String> messages = new ArrayBlockingQueue<>(10);

        public DummySource() {}

        @Override
        public void start(Map<String, String> map) {
            int numMessages = Integer.parseInt(map.get("num-messages") + "");
            messages = new ArrayBlockingQueue<>(numMessages);
            for (int i = 0; i < numMessages; i++) {
                messages.add("message-" + i);
            }
        }

        @Override
        public List<SourceRecord> poll() throws InterruptedException {
            String message = messages.poll();
            if (message == null) {
                Thread.sleep(1000);
                log.info("Nothing to return");
                return List.of();
            } else {
                return List.of(
                        new SourceRecord(
                                Map.of(),
                                Map.of(),
                                null,
                                0,
                                null,
                                null,
                                null,
                                message,
                                System.currentTimeMillis()));
            }
        }

        @Override
        public void stop() {}

        @Override
        public String version() {
            return "1.0";
        }
    }
}
