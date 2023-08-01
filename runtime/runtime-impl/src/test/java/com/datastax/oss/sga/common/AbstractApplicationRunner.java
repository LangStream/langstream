package com.datastax.oss.sga.common;

import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.runtime.ClusterRuntimeRegistry;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.PluginsRegistry;
import com.datastax.oss.sga.deployer.k8s.agents.AgentResourcesFactory;
import com.datastax.oss.sga.impl.deploy.ApplicationDeployer;
import com.datastax.oss.sga.impl.k8s.tests.KubeTestServer;
import com.datastax.oss.sga.impl.parser.ModelBuilder;
import com.datastax.oss.sga.kafka.extensions.KafkaContainerExtension;
import com.datastax.oss.sga.runtime.agent.AgentRunner;
import com.datastax.oss.sga.runtime.api.agent.RuntimePodConfiguration;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.fabric8.kubernetes.api.model.Secret;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
public abstract class AbstractApplicationRunner {


    @RegisterExtension
    protected static final KubeTestServer kubeServer = new KubeTestServer();

    @RegisterExtension
    protected static final KafkaContainerExtension kafkaContainer = new KafkaContainerExtension();

    protected static ApplicationDeployer applicationDeployer;


    protected record ApplicationRuntime(String tenant, String applicationId, Application applicationInstance, ExecutionPlan implementation, Map<String, Secret> secrets) implements  AutoCloseable {
        public void close() {
            applicationDeployer.delete(tenant, implementation, null);
            Awaitility.await().until(() -> {
                log.info("Waiting for secrets to be deleted. {}", secrets);
                return secrets.isEmpty();
            });
        }
    }

    protected ApplicationRuntime deployApplication(String tenant, String appId,
                                                   Map<String, String> application, String ... expectedAgents) throws Exception {

        kubeServer.spyAgentCustomResources(tenant, expectedAgents);
        final Map<String, Secret> secrets = kubeServer.spyAgentCustomResourcesSecrets(tenant, expectedAgents);

        Application applicationInstance = ModelBuilder
                .buildApplicationInstance(application);

        ExecutionPlan implementation = applicationDeployer.createImplementation(appId, applicationInstance);

        applicationDeployer.deploy(tenant, implementation, null);

        return new ApplicationRuntime(tenant, appId, applicationInstance, implementation, secrets);
    }

    protected String buildInstanceYaml() {
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
         applicationDeployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();
    }


    protected KafkaProducer createProducer() {
        return new KafkaProducer<String, String>(
                Map.of("bootstrap.servers", kafkaContainer.getBootstrapServers(),
                        "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                        "value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        );
    }


    protected void sendMessage(String topic, Object content, KafkaProducer producer) throws Exception {
        sendMessage(topic, content, List.of(), producer);
    }

    protected void sendMessage(String topic, Object content, List<Header> headers , KafkaProducer producer) throws Exception {
        producer
                .send(new ProducerRecord<>(
                        topic,
                        null,
                        System.currentTimeMillis(),
                        "key",
                        content,
                        headers))
                .get();
        producer.flush();
    }

    protected List<ConsumerRecord> waitForMessages(KafkaConsumer consumer,
                                                   List<Object> expected) throws Exception {
        List<ConsumerRecord> result = new ArrayList<>();
        List<Object> received = new ArrayList<>();

        Awaitility.await().untilAsserted(() -> {
                    ConsumerRecords<String, String> poll = consumer.poll(Duration.ofSeconds(2));
                    for (ConsumerRecord record : poll) {
                        log.info("Received message {}", record);
                        received.add(record.value());
                        result.add(record);
                    }
                    log.info("Result: {}", received);
                    received.forEach(r -> {
                        log.info("Received |{}|", r);
                    });

                    assertEquals(expected.size(), received.size());
                    for (int i = 0; i < expected.size(); i++) {
                        Object expectedValue = expected.get(i);
                        Object actualValue = received.get(i);
                        if (expectedValue instanceof byte[]) {
                            assertArrayEquals((byte[]) expectedValue, (byte[]) actualValue);
                        } else {
                            assertEquals(expectedValue, actualValue);
                        }
                    }
                }
        );

        return result;
    }

    protected void executeAgentRunners(ApplicationRuntime runtime) throws Exception {
        List<RuntimePodConfiguration> pods = new ArrayList<>();
        runtime.secrets().forEach((key, secret) -> {
            RuntimePodConfiguration runtimePodConfiguration =
                    AgentResourcesFactory.readRuntimePodConfigurationFromSecret(secret);
            log.info("Pod configuration {} = {}", key, runtimePodConfiguration);
            pods.add(runtimePodConfiguration);
        });
        // execute all the pods
        ExecutorService executorService = Executors.newCachedThreadPool();
        List<CompletableFuture> futures = new ArrayList<>();
        for (RuntimePodConfiguration podConfiguration : pods) {
            CompletableFuture<?> handle = new CompletableFuture<>();
            executorService.submit(() -> {
                Thread.currentThread().setName(podConfiguration.agent().agentId() + "runner");
                try {
                    log.info("AgentPod {} Started", podConfiguration.agent().agentId());
                    AgentRunner.run(podConfiguration, null, null, 10);
                    handle.complete(null);
                } catch (Throwable error) {
                    log.error("Error {}", error);
                    handle.completeExceptionally(error);
                } finally {
                    log.info("AgentPod {} Stopoed", podConfiguration.agent().agentId());
                }
            });
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.SECONDS);
    }

    protected KafkaConsumer createConsumer(String topic) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(
                Map.of("bootstrap.servers", kafkaContainer.getBootstrapServers(),
                        "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                        "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                        "group.id", "testgroup",
                        "auto.offset.reset", "earliest")
        );
        consumer.subscribe(List.of(topic));
        return consumer;
    }


    protected static AdminClient getKafkaAdmin() {
        return kafkaContainer.getAdmin();
    }

    @AfterAll
    public static void teardown() {
        if (applicationDeployer != null) {
            // this closes the kubernetes client
            applicationDeployer.close();
        }
    }
}
