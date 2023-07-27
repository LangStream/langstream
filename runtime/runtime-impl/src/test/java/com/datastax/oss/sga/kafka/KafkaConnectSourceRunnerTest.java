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
import io.fabric8.kubernetes.api.model.Secret;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class KafkaConnectSourceRunnerTest {

    private static KafkaContainer kafkaContainer;
    private static AdminClient admin;

    @RegisterExtension
    static final KubeTestServer kubeServer = new KubeTestServer();

    @Test
    @Disabled
    public void testRunKafkaConnectSource() throws Exception {
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
                                """.formatted(DummySourceConnector.class.getName())));

        @Cleanup ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation("app", applicationInstance);
        assertTrue(implementation.getConnectionImplementation(module,
                Connection.from(TopicDefinition.fromName("output-topic"))) instanceof KafkaTopic);

        deployer.deploy(tenant, implementation, null);
        assertEquals(1, secrets.size());
        final Secret secret = secrets.values().iterator().next();
        final RuntimePodConfiguration runtimePodConfiguration =
                AgentResourcesFactory.readRuntimePodConfigurationFromSecret(secret);

        try (KafkaConsumer<String, String>consumer = new KafkaConsumer<String, String>(
                     Map.of("bootstrap.servers", kafkaContainer.getBootstrapServers(),
        "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                "value.deserializer", " org.apache.kafka.common.serialization.StringDeserializer",
                "group.id","testgroup",
                "auto.offset.reset", "earliest"))) {
            consumer.subscribe(List.of("output-topic"));

            AgentRunner.run(runtimePodConfiguration, null, null, 5);

            TestUtils.waitForMessages(consumer, List.of(
                    "message-0", "message-1", "message-2", "message-3", "message-4"));
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

    public static final class DummySource extends SourceTask {

        BlockingQueue<String> messages = new ArrayBlockingQueue<>(10);

        public DummySource() {
        }

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
                return List.of(new SourceRecord(
                        Map.of(),
                        Map.of(),
                        null,
                        0,
                        null,
                        null,
                        null,
                        message,
                        System.currentTimeMillis()
                ));
            }
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