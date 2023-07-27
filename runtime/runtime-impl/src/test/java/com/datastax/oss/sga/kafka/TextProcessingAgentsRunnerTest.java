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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class TextProcessingAgentsRunnerTest {

    private static KafkaContainer kafkaContainer;
    private static AdminClient admin;

    @RegisterExtension
    static final KubeTestServer kubeServer = new KubeTestServer();

    @Test
    public void testFullLanguageProcessingPipeline() throws Exception {
        String tenant = "tenant";
        kubeServer.spyAgentCustomResources(tenant, "app-text-extractor1");
        final Map<String, Secret> secrets = kubeServer.spyAgentCustomResourcesSecrets(tenant,  "app-text-extractor1");

        Application applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of("instance.yaml",
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
                                """));

        @Cleanup ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation("app", applicationInstance);
        log.info("Implementation {}", implementation);
        assertTrue(implementation.getConnectionImplementation(module,
                Connection.from(TopicDefinition.fromName("input-topic"))) instanceof KafkaTopic);


        deployer.deploy(tenant, implementation, null);
        assertEquals(1, secrets.size());
        List<RuntimePodConfiguration> pods = new ArrayList<>();
        secrets.values().forEach(secret -> {
            RuntimePodConfiguration runtimePodConfiguration =
                    AgentResourcesFactory.readRuntimePodConfigurationFromSecret(secret);
            log.info("Pod configuration {}", runtimePodConfiguration);
            pods.add(runtimePodConfiguration);
        });

        try (KafkaProducer<String, String> producer = new KafkaProducer<String, String>(
                Map.of("bootstrap.servers", kafkaContainer.getBootstrapServers(),
                        "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                        "value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        );
             KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(
                     Map.of("bootstrap.servers", kafkaContainer.getBootstrapServers(),
                             "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                             "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                             "group.id","testgroup",
                             "auto.offset.reset", "earliest")
             )) {
            consumer.subscribe(List.of("output-topic"));


            // produce two messages to the input-topic
            producer
                    .send(new ProducerRecord<>(
                            "input-topic",
                            null,
                            "key",
                            "Questo testo è scritto in Italiano.",
                            List.of()))
                    .get();
            producer
                    .send(new ProducerRecord<>(
                            "input-topic",
                            null,
                            "key",
                            "This text is written in English, but it is very long,\nso you may want to split it into chunks.",
                            List.of()))
                    .get();
            producer.flush();

            // execute all the pods
            ExecutorService executorService = Executors.newCachedThreadPool();
            List<CompletableFuture> futures = new ArrayList<>();
            for (RuntimePodConfiguration podConfiguration : pods) {
                CompletableFuture<?> handle = new CompletableFuture<>();
                executorService.submit(() -> {
                    Thread.currentThread().setName(podConfiguration.agent().agentId() + "runner");
                    try {
                        AgentRunner.run(podConfiguration, null, null, 10);
                        handle.complete(null);
                    } catch (Throwable error) {
                        log.error("Error {}", error);
                        handle.completeExceptionally(error);
                    }
                });
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            executorService.shutdown();
            executorService.awaitTermination(5, TimeUnit.SECONDS);


            TestUtils.waitForMessages(consumer, List.of("this   text   is   written   in   english,   but",
                    "it   is   very   long,",
                    "so you may want to split it into chunks."));

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
}