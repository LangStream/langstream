package com.datastax.oss.sga.kafka;

import com.dastastax.oss.sga.kafka.runtime.KafkaTopic;
import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.Connection;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.TopicDefinition;
import com.datastax.oss.sga.api.runtime.ClusterRuntimeRegistry;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.PluginsRegistry;
import com.datastax.oss.sga.deployer.k8s.agents.AgentResourcesFactory;
import com.datastax.oss.sga.impl.deploy.ApplicationDeployer;
import com.datastax.oss.sga.impl.k8s.tests.KubeTestServer;
import com.datastax.oss.sga.impl.parser.ModelBuilder;
import com.datastax.oss.sga.runtime.agent.AgentRunner;
import com.datastax.oss.sga.runtime.api.agent.RuntimePodConfiguration;
import com.google.common.base.Strings;
import io.fabric8.kubernetes.api.model.Secret;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class KafkaConnectRunnerTest {

    private static KafkaContainer kafkaContainer;
    private static AdminClient admin;

    @RegisterExtension
    static final KubeTestServer kubeServer = new KubeTestServer();


    @Test
    public void testRunSnowflakeKafkaConnectSink() throws Exception {
        String tenant = "tenant";
        kubeServer.spyAgentCustomResources(tenant, "app-step1");
        final Map<String, Secret> secrets = kubeServer.spyAgentCustomResourcesSecrets(tenant, "app-step1");

        String sfUrl = System.getProperty("sf.url", "dmb76871.us-east-1.snowflakecomputing.com:443");
        String sfUser = System.getProperty("sf.user", "test_connector_user_1");
        String sfKey = System.getProperty("sf.key");
        String sfDatabase = System.getProperty("sf.database", "test_db");
        String sfSchema = System.getProperty("sf.schema", "test_schema");

        if (Strings.isNullOrEmpty(sfKey)) {
            log.error("SF configuration is missing, skipping the test");
            return;
        }

        Application applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of("instance.yaml",
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
                ));

        ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation("app", applicationInstance);
        assertTrue(implementation.getConnectionImplementation(module,
                Connection.from(TopicDefinition.fromName("input-topic"))) instanceof KafkaTopic);

        deployer.deploy(tenant, implementation, null);
        assertEquals(1, secrets.size());
        final Secret secret = secrets.values().iterator().next();
        final RuntimePodConfiguration runtimePodConfiguration =
                AgentResourcesFactory.readRuntimePodConfigurationFromSecret(secret);

        try (KafkaProducer<String, String> producer = new KafkaProducer<String, String>(
                Map.of("bootstrap.servers", kafkaContainer.getBootstrapServers(),
                        "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                        "value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        )) {

            for (int i = 0; i < 20; i++) {
                producer
                        .send(new ProducerRecord<>(
                                "input-topic",
                                0,
                                "key",
                                "{\"name\": \"some json name " + i + "\", \"description\": \"some description\"}"))
                        .get();
            }
            producer.flush();

            AgentRunner.run(runtimePodConfiguration, null, null, 5);
        }
        //TODO: validate snowflake automatically
    }

    @Test
    public void testRunKafkaConnectSink() throws Exception {
        String tenant = "tenant";
        kubeServer.spyAgentCustomResources(tenant, "app-step1");
        final Map<String, Secret> secrets = kubeServer.spyAgentCustomResourcesSecrets(tenant, "app-step1");

        Application applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of("instance.yaml",
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
                                """.formatted(DummySinkConnector.class.getName())));

        @Cleanup ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation("app", applicationInstance);
        assertTrue(implementation.getConnectionImplementation(module,
                Connection.from(TopicDefinition.fromName("input-topic"))) instanceof KafkaTopic);

        deployer.deploy(tenant, implementation, null);
        assertEquals(1, secrets.size());
        final Secret secret = secrets.values().iterator().next();
        final RuntimePodConfiguration runtimePodConfiguration =
                AgentResourcesFactory.readRuntimePodConfigurationFromSecret(secret);

        try (KafkaProducer<String, String> producer = new KafkaProducer<String, String>(
                Map.of("bootstrap.servers", kafkaContainer.getBootstrapServers(),
                "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                "value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        )) {

            // produce one message to the input-topic
            producer
                .send(new ProducerRecord<>(
                    "input-topic",
                    null,
                    "key",
                    "{\"name\": \"some name\", \"description\": \"some description\"}",
                    List.of(new RecordHeader("header-key", "header-value".getBytes(StandardCharsets.UTF_8)))))
                .get();
            producer.flush();

            AgentRunner.run(runtimePodConfiguration, null, null, 5);

            Awaitility.await().untilAsserted(() -> {
                    DummySink.receivedRecords.forEach(r -> log.info("Received record: {}", r));
                    assertTrue(DummySink.receivedRecords.size() >= 1);
            });

        }

    }

    private static String buildInstanceYaml() {
        return """
                instance:
                  streamingCluster:
                    type: "kafka"
                    configuration:
                      admin:
                        bootstrap.servers: "%s"
                  computeCluster:
                     type: "kubernetes"
                """.formatted(kafkaContainer.getBootstrapServers());
    }


    @BeforeAll
    public static void setup() throws Exception {
        kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"))
                .withLogConsumer(new Consumer<OutputFrame>() {
                    @Override
                    public void accept(OutputFrame outputFrame) {
                        log.info("kafka> {}", outputFrame.getUtf8String().trim());
                    }
                });
        // start Pulsar and wait for it to be ready to accept requests
        kafkaContainer.start();
        admin =
                AdminClient.create(Map.of("bootstrap.servers", kafkaContainer.getBootstrapServers()));
    }

    @AfterAll
    public static void teardown() {
        if (admin != null) {
            admin.close();
        }
        if (kafkaContainer != null) {
            kafkaContainer.close();
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