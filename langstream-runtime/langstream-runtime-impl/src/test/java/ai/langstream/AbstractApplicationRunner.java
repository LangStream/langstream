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
package ai.langstream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.langstream.api.model.Application;
import ai.langstream.api.runner.assets.AssetManagerRegistry;
import ai.langstream.api.runner.topics.TopicConnectionsRuntimeRegistry;
import ai.langstream.api.runtime.ClusterRuntimeRegistry;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.deployer.k8s.agents.AgentResourcesFactory;
import ai.langstream.impl.deploy.ApplicationDeployer;
import ai.langstream.impl.k8s.tests.KubeTestServer;
import ai.langstream.impl.nar.NarFileHandler;
import ai.langstream.impl.parser.ModelBuilder;
import ai.langstream.kafka.extensions.KafkaContainerExtension;
import ai.langstream.runtime.agent.AgentRunner;
import ai.langstream.runtime.agent.api.AgentInfo;
import ai.langstream.runtime.api.agent.RuntimePodConfiguration;
import io.fabric8.kubernetes.api.model.Secret;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;

@Slf4j
public abstract class AbstractApplicationRunner {

    public static final Path agentsDirectory;

    static {
        agentsDirectory = Path.of(System.getProperty("user.dir"), "target", "agents");
        log.info("Agents directory is {}", agentsDirectory);
    }

    @RegisterExtension protected static final KubeTestServer kubeServer = new KubeTestServer();

    @RegisterExtension
    protected static final KafkaContainerExtension kafkaContainer = new KafkaContainerExtension();

    protected static ApplicationDeployer applicationDeployer;
    private static NarFileHandler narFileHandler;
    private static TopicConnectionsRuntimeRegistry topicConnectionsRuntimeRegistry;

    protected record ApplicationRuntime(
            String tenant,
            String applicationId,
            Application applicationInstance,
            ExecutionPlan implementation,
            Map<String, Secret> secrets)
            implements AutoCloseable {

        public <T> T getGlobal(String key) {
            return (T) implementation.getApplication().getInstance().globals().get(key);
        }

        public void close() {
            applicationDeployer.delete(tenant, implementation, null);
            Awaitility.await()
                    .until(
                            () -> {
                                log.info("Waiting for secrets to be deleted. {}", secrets);
                                return secrets.isEmpty();
                            });
            // this is a workaround, we want to clean up the env
            topicConnectionsRuntimeRegistry
                    .getTopicConnectionsRuntime(
                            implementation.getApplication().getInstance().streamingCluster())
                    .asTopicConnectionsRuntime()
                    .delete(implementation);
        }
    }

    protected ApplicationRuntime deployApplication(
            String tenant,
            String appId,
            Map<String, String> application,
            String instance,
            String... expectedAgents)
            throws Exception {
        return deployApplicationWithSecrets(
                tenant, appId, application, instance, null, expectedAgents);
    }

    protected ApplicationRuntime deployApplicationWithSecrets(
            String tenant,
            String appId,
            Map<String, String> application,
            String instance,
            String secretsContents,
            String... expectedAgents)
            throws Exception {

        kubeServer.spyAgentCustomResources(tenant, expectedAgents);
        final Map<String, Secret> secrets =
                kubeServer.spyAgentCustomResourcesSecrets(tenant, expectedAgents);

        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(application, instance, secretsContents)
                        .getApplication();

        ExecutionPlan implementation =
                applicationDeployer.createImplementation(appId, applicationInstance);

        applicationDeployer.setup(tenant, implementation);
        applicationDeployer.deploy(tenant, implementation, null);

        return new ApplicationRuntime(tenant, appId, applicationInstance, implementation, secrets);
    }

    protected String buildInstanceYaml() {
        String inputTopic = "input-topic-" + UUID.randomUUID();
        String outputTopic = "output-topic-" + UUID.randomUUID();
        String streamTopic = "stream-topic-" + UUID.randomUUID();
        return """
                instance:
                  globals:
                    input-topic: %s
                    output-topic: %s
                    stream-topic: %s
                  streamingCluster:
                    type: "kafka"
                    configuration:
                      admin:
                        bootstrap.servers: "%s"
                  computeCluster:
                     type: "kubernetes"
                """
                .formatted(
                        inputTopic, outputTopic, streamTopic, kafkaContainer.getBootstrapServers());
    }

    @BeforeAll
    public static void setup() throws Exception {
        narFileHandler =
                new NarFileHandler(
                        agentsDirectory, List.of(), Thread.currentThread().getContextClassLoader());
        topicConnectionsRuntimeRegistry = new TopicConnectionsRuntimeRegistry();
        narFileHandler.scan();
        topicConnectionsRuntimeRegistry.setPackageLoader(narFileHandler);
        final AssetManagerRegistry assetManagerRegistry = new AssetManagerRegistry();
        assetManagerRegistry.setAssetManagerPackageLoader(narFileHandler);
        applicationDeployer =
                ApplicationDeployer.builder()
                        .registry(new ClusterRuntimeRegistry())
                        .pluginsRegistry(new PluginsRegistry())
                        .topicConnectionsRuntimeRegistry(topicConnectionsRuntimeRegistry)
                        .assetManagerRegistry(assetManagerRegistry)
                        .build();
    }

    protected KafkaProducer<String, String> createProducer() {
        return new KafkaProducer<>(
                Map.of(
                        "bootstrap.servers",
                        kafkaContainer.getBootstrapServers(),
                        "key.serializer",
                        "org.apache.kafka.common.serialization.StringSerializer",
                        "value.serializer",
                        "org.apache.kafka.common.serialization.StringSerializer"));
    }

    protected void sendMessage(String topic, Object content, KafkaProducer producer)
            throws Exception {
        sendMessage(topic, content, List.of(), producer);
    }

    protected void sendMessage(
            String topic, Object content, List<Header> headers, KafkaProducer producer)
            throws Exception {
        sendMessage(topic, "key", content, headers, producer);
    }

    protected void sendMessage(
            String topic, Object key, Object content, List<Header> headers, KafkaProducer producer)
            throws Exception {
        producer.send(
                        new ProducerRecord<>(
                                topic, null, System.currentTimeMillis(), key, content, headers))
                .get();
        producer.flush();
    }

    protected List<ConsumerRecord> waitForMessages(KafkaConsumer consumer, List<?> expected) {
        List<ConsumerRecord> result = new ArrayList<>();
        List<Object> received = new ArrayList<>();

        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            ConsumerRecords<String, String> poll =
                                    consumer.poll(Duration.ofSeconds(2));
                            for (ConsumerRecord record : poll) {
                                log.info("Received message {}", record);
                                received.add(record.value());
                                result.add(record);
                            }
                            log.info("Result:  {}", received);
                            received.forEach(r -> log.info("Received |{}|", r));

                            assertEquals(expected.size(), received.size());
                            for (int i = 0; i < expected.size(); i++) {
                                Object expectedValue = expected.get(i);
                                Object actualValue = received.get(i);
                                if (expectedValue instanceof Consumer fn) {
                                    fn.accept(actualValue);
                                } else if (expectedValue instanceof byte[]) {
                                    assertArrayEquals((byte[]) expectedValue, (byte[]) actualValue);
                                } else {
                                    assertEquals(expectedValue, actualValue);
                                }
                            }
                        });

        return result;
    }

    protected List<ConsumerRecord> waitForMessagesInAnyOrder(
            KafkaConsumer consumer, Collection<String> expected) {
        List<ConsumerRecord> result = new ArrayList<>();
        List<Object> received = new ArrayList<>();

        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            ConsumerRecords<String, String> poll =
                                    consumer.poll(Duration.ofSeconds(2));
                            for (ConsumerRecord record : poll) {
                                log.info("Received message {}", record);
                                received.add(record.value());
                                result.add(record);
                            }
                            log.info("Result: {}", received);
                            received.forEach(r -> log.info("Received |{}|", r));

                            assertEquals(expected.size(), received.size());
                            for (Object expectedValue : expected) {
                                // this doesn't work for byte[]
                                assertFalse(expectedValue instanceof byte[]);
                                assertTrue(
                                        received.contains(expectedValue),
                                        "Expected value "
                                                + expectedValue
                                                + " not found in "
                                                + received);
                            }

                            for (Object receivedValue : received) {
                                // this doesn't work for byte[]
                                assertFalse(receivedValue instanceof byte[]);
                                assertTrue(
                                        expected.contains(receivedValue),
                                        "Received value "
                                                + receivedValue
                                                + " not found in "
                                                + expected);
                            }
                        });

        return result;
    }

    public record AgentRunResult(Map<String, AgentInfo> info) {}

    protected AgentRunResult executeAgentRunners(ApplicationRuntime runtime) throws Exception {
        String runnerExecutionId = UUID.randomUUID().toString();
        log.info(
                "{} Starting Agent Runners. Running {} pods",
                runnerExecutionId,
                runtime.secrets.size());
        Map<String, AgentInfo> allAgentsInfo = new ConcurrentHashMap<>();
        try {
            List<RuntimePodConfiguration> pods = new ArrayList<>();
            runtime.secrets()
                    .forEach(
                            (key, secret) -> {
                                RuntimePodConfiguration runtimePodConfiguration =
                                        AgentResourcesFactory.readRuntimePodConfigurationFromSecret(
                                                secret);
                                log.info(
                                        "{} Pod configuration {} = {}",
                                        runnerExecutionId,
                                        key,
                                        runtimePodConfiguration);
                                pods.add(runtimePodConfiguration);
                            });
            // execute all the pods
            ExecutorService executorService = Executors.newCachedThreadPool();
            List<CompletableFuture> futures = new ArrayList<>();
            for (RuntimePodConfiguration podConfiguration : pods) {
                CompletableFuture<?> handle = new CompletableFuture<>();
                futures.add(handle);
                executorService.submit(
                        () -> {
                            String originalName = Thread.currentThread().getName();
                            Thread.currentThread()
                                    .setName(
                                            podConfiguration.agent().agentId()
                                                    + "runner-tid-"
                                                    + runnerExecutionId);
                            try {
                                log.info(
                                        "{} AgentPod {} Started",
                                        runnerExecutionId,
                                        podConfiguration.agent().agentId());
                                AgentInfo agentInfo = new AgentInfo();
                                allAgentsInfo.put(podConfiguration.agent().agentId(), agentInfo);
                                AtomicInteger numLoops = new AtomicInteger();
                                AgentRunner.runAgent(
                                        podConfiguration,
                                        null,
                                        agentsDirectory,
                                        agentInfo,
                                        () -> {
                                            log.info("Num loops {}", numLoops.get());
                                            return numLoops.incrementAndGet() <= 10;
                                        },
                                        () -> validateAgentInfoBeforeStop(agentInfo),
                                        false);
                                List<?> infos = agentInfo.serveWorkerStatus();
                                log.info(
                                        "{} AgentPod {} AgentInfo {}",
                                        runnerExecutionId,
                                        podConfiguration.agent().agentId(),
                                        infos);
                                handle.complete(null);
                            } catch (Throwable error) {
                                log.error(
                                        "{} Error on AgentPod {}",
                                        runnerExecutionId,
                                        podConfiguration.agent().agentId(),
                                        error);
                                handle.completeExceptionally(error);
                            } finally {
                                log.info(
                                        "{} AgentPod {} finished",
                                        runnerExecutionId,
                                        podConfiguration.agent().agentId());
                                Thread.currentThread().setName(originalName);
                            }
                        });
            }
            try {
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
            } catch (ExecutionException executionException) {
                log.error(
                        "Some error occurred while executing the agent",
                        executionException.getCause());
                // unwrap the exception in order to easily perform assertions
                if (executionException.getCause() instanceof Exception) {
                    throw (Exception) executionException.getCause();
                } else {
                    throw executionException;
                }
            }
            executorService.shutdown();
            assertTrue(
                    executorService.awaitTermination(1, TimeUnit.MINUTES),
                    "the pods didn't finish in time");
        } finally {
            log.info("{} Agent Runners Stopped", runnerExecutionId);
        }
        return new AgentRunResult(allAgentsInfo);
    }

    private volatile boolean validateConsumerOffsets = true;

    public boolean isValidateConsumerOffsets() {
        return validateConsumerOffsets;
    }

    public void setValidateConsumerOffsets(boolean validateConsumerOffsets) {
        this.validateConsumerOffsets = validateConsumerOffsets;
    }

    private void validateAgentInfoBeforeStop(AgentInfo agentInfo) {
        if (!validateConsumerOffsets) {
            return;
        }
        agentInfo
                .serveWorkerStatus()
                .forEach(
                        workerStatus -> {
                            String agentType = workerStatus.getAgentType();
                            log.info("Checking Agent type {}", agentType);
                            switch (agentType) {
                                case "topic-source":
                                    Map<String, Object> info = workerStatus.getInfo();
                                    log.info("Topic source info {}", info);
                                    Map<String, Object> consumerInfo =
                                            (Map<String, Object>) info.get("consumer");
                                    if (consumerInfo != null) {
                                        Map<String, Object> committedOffsets =
                                                (Map<String, Object>)
                                                        consumerInfo.get("committedOffsets");
                                        log.info("Committed offsets {}", committedOffsets);
                                        committedOffsets.forEach(
                                                (topic, offset) -> {
                                                    assertNotNull(offset);
                                                    assertTrue(((Number) offset).intValue() >= 0);
                                                });
                                        Map<String, Object> uncommittedOffsets =
                                                (Map<String, Object>)
                                                        consumerInfo.get("uncommittedOffsets");
                                        log.info("Uncommitted offsets {}", uncommittedOffsets);
                                        uncommittedOffsets.forEach(
                                                (topic, number) -> {
                                                    assertNotNull(number);
                                                    assertTrue(
                                                            ((Number) number).intValue() <= 0,
                                                            "for topic "
                                                                    + topic
                                                                    + " we have some uncommitted offsets: "
                                                                    + number);
                                                });
                                    }
                                default:
                                    // ignore
                            }
                        });
    }

    protected KafkaConsumer<String, String> createConsumer(String topic) {
        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(
                        Map.of(
                                "bootstrap.servers",
                                kafkaContainer.getBootstrapServers(),
                                "key.deserializer",
                                "org.apache.kafka.common.serialization.StringDeserializer",
                                "value.deserializer",
                                "org.apache.kafka.common.serialization.StringDeserializer",
                                "group.id",
                                "testgroup-" + UUID.randomUUID(),
                                "auto.offset.reset",
                                "earliest"));
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
        if (narFileHandler != null) {
            narFileHandler.close();
        }
    }
}
