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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.base.Strings;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Slf4j
class KafkaConnectSinkRunnerIT extends AbstractKafkaApplicationRunner {

    @Test
    @Disabled
    public void testRunSnowflakeKafkaConnectSink() throws Exception {
        String tenant = "tenant";
        String[] expectedAgents = {"app-step1"};

        String sfUrl =
                System.getProperty("sf.url", "dmb76871.us-east-1.snowflakecomputing.com:443");
        String sfUser = System.getProperty("sf.user", "test_connector_user_1");
        String sfKey = System.getProperty("sf.key");
        String sfDatabase = System.getProperty("sf.database", "test_db");
        String sfSchema = System.getProperty("sf.schema", "test_schema");

        if (Strings.isNullOrEmpty(sfKey)) {
            log.error("SF configuration is missing, skipping the test");
            return;
        }

        Map<String, String> application =
                Map.of(
                        "module.yaml",
                        """
                                module: "module-1"
                                id: "pipeline-1"
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "sink1"
                                    id: "step1"
                                    type: "sink"
                                    input: "input-topic"
                                    configuration:
                                      name: "snowflake-sink-kv"
                                      connector.class: "com.snowflake.kafka.connector.SnowflakeSinkConnector"
                                      tasks.max: "1"
                                      buffer.count.records: "10"
                                      buffer.flush.time: "100"
                                      buffer.size.bytes: "100"
                                      snowflake.url.name: "%s"
                                      snowflake.user.name: "%s"
                                      snowflake.private.key: "%s"
                                      snowflake.database.name: "%s"
                                      snowflake.schema.name: "%s"
                                      key.converter: "org.apache.kafka.connect.storage.StringConverter"
                                      #value.converter: "org.apache.kafka.connect.storage.StringConverter"
                                      value.converter: "com.snowflake.kafka.connector.records.SnowflakeJsonConverter"

                                      adapterConfig:
                                        batchSize: 2
                                        lingerTimeMs: 1000
                                """
                                .formatted(sfUrl, sfUser, sfKey, sfDatabase, sfSchema));

        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer()) {
                for (int i = 0; i < 20; i++) {
                    sendMessage(
                            "input-topic",
                            "{\"name\": \"some json name "
                                    + i
                                    + "\", \"description\": \"some description\"}",
                            producer);
                }
                producer.flush();

                executeAgentRunners(applicationRuntime);
            }
        }
        // TODO: validate snowflake automatically
    }

    @Test
    public void testRunKafkaConnectSinkFailOnErr() throws Exception {
        String tenant = "tenant2";
        String[] expectedAgents = {"app-step2"};

        DummySink.receivedRecords.clear();

        String onFailure = "fail";
        Map<String, String> application =
                Map.of(
                        "module.yaml",
                        """
                                module: "module-2"
                                id: "pipeline-2"
                                errors:
                                  on-failure: "%s"
                                topics:
                                  - name: "input-topic2"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "sink2"
                                    id: "step2"
                                    type: "sink"
                                    input: "input-topic2"
                                    configuration:
                                      adapterConfig:
                                        __test_inject_conversion_error: "1"
                                      connector.class: %s
                                      file: /tmp/test.sink.txt
                                """
                                .formatted(onFailure, DummySinkConnector.class.getName()));

        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer()) {
                sendMessage("input-topic2", "err", producer);
                sendMessage(
                        "input-topic2",
                        "{\"name\": \"some name\", \"description\": \"some description\"}",
                        producer);
                try {
                    executeAgentRunners(applicationRuntime);
                    fail();
                } catch (Exception err) {
                    // expected
                    assertEquals("Injected record conversion error", err.getCause().getMessage());
                }
                Thread.sleep(1000);
                // todo: assert on processed counter (incremented before error handling)
                DummySink.receivedRecords.forEach(r -> log.info("Received record: {}", r));
                assertEquals(0, DummySink.receivedRecords.size());
            }
        }
    }

    @Test
    public void testRunKafkaConnectSinkSkipOnErr() throws Exception {
        String tenant = "tenant3";
        String[] expectedAgents = {"app-step3"};

        DummySink.receivedRecords.clear();

        String onFailure = "skip";
        Map<String, String> application =
                Map.of(
                        "module.yaml",
                        """
                                module: "module-3"
                                id: "pipeline-3"
                                errors:
                                  on-failure: "%s"
                                topics:
                                  - name: "input-topic3"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "sink3"
                                    id: "step3"
                                    type: "sink"
                                    input: "input-topic3"
                                    configuration:
                                      adapterConfig:
                                        __test_inject_conversion_error: "1"
                                      connector.class: %s
                                      file: /tmp/test.sink.txt
                                """
                                .formatted(onFailure, DummySinkConnector.class.getName()));

        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer()) {
                sendMessage("input-topic3", "err", producer);
                sendMessage(
                        "input-topic3",
                        "{\"name\": \"some name\", \"description\": \"some description\"}",
                        producer);
                executeAgentRunners(applicationRuntime);
                Awaitility.await()
                        .untilAsserted(
                                () -> {
                                    DummySink.receivedRecords.forEach(
                                            r -> log.info("Received record: {}", r));
                                    // todo: assert on processed counter (incremented before error
                                    // handling)
                                    assertEquals(1, DummySink.receivedRecords.size());
                                });
            }
        }
    }

    @Test
    public void testRunKafkaConnectSinkDlqOnErr() throws Exception {
        String tenant = "tenant4";
        String[] expectedAgents = {"app-step4"};

        DummySink.receivedRecords.clear();

        String onFailure = "dead-letter";
        Map<String, String> application =
                Map.of(
                        "module.yaml",
                        """
                                module: "module-4"
                                id: "pipeline-4"
                                errors:
                                  on-failure: "%s"
                                topics:
                                  - name: "input-topic4"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "sink4"
                                    id: "step4"
                                    type: "sink"
                                    input: "input-topic4"
                                    configuration:
                                      adapterConfig:
                                        __test_inject_conversion_error: "1"
                                      connector.class: %s
                                      file: /tmp/test.sink.txt
                                """
                                .formatted(onFailure, DummySinkConnector.class.getName()));

        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer()) {
                sendMessage("input-topic4", "err", producer);
                sendMessage(
                        "input-topic4",
                        "{\"name\": \"some name\", \"description\": \"some description\"}",
                        producer);
                executeAgentRunners(applicationRuntime);
                Awaitility.await()
                        .untilAsserted(
                                () -> {
                                    DummySink.receivedRecords.forEach(
                                            r -> log.info("Received record: {}", r));
                                    // todo: assert on processed counter (incremented before error
                                    // handling)
                                    // todo: check DLQ's content
                                    assertEquals(1, DummySink.receivedRecords.size());
                                });
            }
        }
    }

    public static final class DummySinkConnector extends SinkConnector {
        @Override
        public void start(Map<String, String> map) {}

        @Override
        public Class<? extends Task> taskClass() {
            return DummySink.class;
        }

        @Override
        public List<Map<String, String>> taskConfigs(int i) {
            return List.of(Map.of());
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

    public static final class DummySink extends org.apache.kafka.connect.sink.SinkTask {

        static final List<SinkRecord> receivedRecords = new CopyOnWriteArrayList<>();

        @Override
        public void start(Map<String, String> map) {}

        @Override
        public void put(Collection<SinkRecord> collection) {
            log.info("Sink records {}", collection);
            receivedRecords.addAll(collection);
        }

        @Override
        public void stop() {}

        @Override
        public String version() {
            return "1.0";
        }
    }
}
