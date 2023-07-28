package com.datastax.oss.sga.kafka;

import com.datastax.oss.sga.common.AbstractApplicationRunner;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class KafkaConnectSinkRunnerTest extends AbstractApplicationRunner  {


    @Test
    @Disabled
    public void testRunSnowflakeKafkaConnectSink() throws Exception {
        String tenant = "tenant";
        String[] expectedAgents = {"app-step1"};

        String sfUrl = System.getProperty("sf.url", "dmb76871.us-east-1.snowflakecomputing.com:443");
        String sfUser = System.getProperty("sf.user", "test_connector_user_1");
        String sfKey = System.getProperty("sf.key");
        String sfDatabase = System.getProperty("sf.database", "test_db");
        String sfSchema = System.getProperty("sf.schema", "test_schema");

        if (Strings.isNullOrEmpty(sfKey)) {
            log.error("SF configuration is missing, skipping the test");
            return;
        }

        Map<String, String> application = Map.of("instance.yaml",
                        buildInstanceYaml(),
                        "module.yaml", """
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
                                """.formatted(sfUrl, sfUser, sfKey, sfDatabase, sfSchema)
                );

        try (ApplicationRuntime applicationRuntime = deployApplication(tenant, "app", application, expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer()) {
                for (int i = 0; i < 20; i++) {
                    sendMessage("input-topic", "{\"name\": \"some json name " + i + "\", \"description\": \"some description\"}", producer);
                }
                producer.flush();

                executeAgentRunners(applicationRuntime);
            }
        }
        //TODO: validate snowflake automatically
    }

    @Test
    public void testRunKafkaConnectSink() throws Exception {

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
                                pipeline:
                                  - name: "sink1"
                                    id: "step1"
                                    type: "sink"
                                    input: "input-topic"
                                    configuration:
                                      connector.class: %s                                        
                                      file: /tmp/test.sink.txt
                                """.formatted(DummySinkConnector.class.getName()));

        try (ApplicationRuntime applicationRuntime = deployApplication(tenant, "app", application, expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer()) {
                sendMessage("input-topic", "{\"name\": \"some name\", \"description\": \"some description\"}", producer);
                executeAgentRunners(applicationRuntime);
                Awaitility.await().untilAsserted(() -> {
                    DummySink.receivedRecords.forEach(r -> log.info("Received record: {}", r));
                    assertTrue(DummySink.receivedRecords.size() >= 1);
                });
            }
        }

    }

    public static final class DummySinkConnector extends SinkConnector {
        @Override
        public void start(Map<String, String> map) {
        }

        @Override
        public Class<? extends Task> taskClass() {
            return DummySink.class;
        }

        @Override
        public List<Map<String, String>> taskConfigs(int i) {
            return List.of(Map.of());
        }

        @Override
        public void stop() {
        }

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
        public void start(Map<String, String> map) {
        }

        @Override
        public void put(Collection<SinkRecord> collection) {
            log.info("Sink records {}", collection);
            receivedRecords.addAll(collection);
        }

        @Override
        public void stop() {
        }

        @Override
        public String version() {
            return "1.0";
        }
    }

}