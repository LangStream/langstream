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
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.awaitility.Awaitility;
import org.checkerframework.checker.units.qual.C;
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
class TikaAgentsRunnerTest {

    private static KafkaContainer kafkaContainer;
    private static AdminClient admin;

    @RegisterExtension
    static final KubeTestServer kubeServer = new KubeTestServer();


    @Test
    public void testLanguageDetectionPipeline() throws Exception {
        String tenant = "tenant";
        kubeServer.spyAgentCustomResources(tenant, "app-step1", "app-keep-only-english");
        final Map<String, Secret> secrets = kubeServer.spyAgentCustomResourcesSecrets(tenant, "app-step1", "app-keep-only-english");

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
                                  - name: "text-extractor"
                                    id: "step1"
                                    type: "text-extractor"
                                    input: "input-topic"                                    
                                    configuration:                                      
                                      param1: "value1"
                                  - name: "language-detector"
                                    id: "step2"
                                    type: "language-detector"                                    
                                    configuration:                                      
                                      param2: "value2"
                                  - name: "keep-only-english"
                                    id: "keep-only-english"
                                    type: "drop"             
                                    output: "output-topic"
                                    configuration:                                      
                                      when: "properties.language != 'en'"
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
        assertEquals(2, secrets.size());
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
                            "This text is written in English",
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


            // receive one message from the output-topic (written by the PodJavaRuntime)
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofSeconds(10));
            assertEquals(poll.count(), 1);
            ConsumerRecord<String, String> record = poll.iterator().next();
            assertEquals("This text is written in English", record.value().trim());
            assertEquals("en", new String(record.headers().lastHeader("language").value(), StandardCharsets.UTF_8));
        }

    }


    @Test
    public void testFullLanguageProcessingPipiline() throws Exception {
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
                                    type: "text-chunker"
                                    configuration:
                                      chunkSize: 60
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


            List<String> received = new ArrayList<>();

            Awaitility.await().until(() -> {
                    ConsumerRecords<String, String> poll = consumer.poll(Duration.ofSeconds(2));
                    for (ConsumerRecord record : poll) {
                        log.info("Received message {}", record);
                        received.add(record.value().toString());
                    }
                    return received.size() >= 2;
                }
            );
            log.info("Result: {}", received);
            received.forEach(r -> {
                log.info("Received |{}|", r);
            });
            assertEquals(List.of("this text is written in english, but it is very long,",
                    "so you may want to split it into chunks."), received);
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