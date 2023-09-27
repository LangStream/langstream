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
package ai.langstream.pulsar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.langstream.AbstractApplicationRunner;
import ai.langstream.api.model.Application;
import ai.langstream.api.model.Connection;
import ai.langstream.api.model.Module;
import ai.langstream.api.model.TopicDefinition;
import ai.langstream.api.runner.topics.TopicConnectionsRuntimeRegistry;
import ai.langstream.api.runtime.ClusterRuntimeRegistry;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.api.runtime.Topic;
import ai.langstream.deployer.k8s.agents.AgentResourcesFactory;
import ai.langstream.impl.deploy.ApplicationDeployer;
import ai.langstream.impl.k8s.tests.KubeTestServer;
import ai.langstream.impl.nar.NarFileHandler;
import ai.langstream.impl.parser.ModelBuilder;
import ai.langstream.runtime.agent.AgentRunner;
import ai.langstream.runtime.agent.api.AgentInfo;
import ai.langstream.runtime.api.agent.RuntimePodConfiguration;
import io.fabric8.kubernetes.api.model.Secret;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.utility.DockerImageName;

@Slf4j
class PulsarRunnerDockerTest {

    private static final String IMAGE = "apachepulsar/pulsar:3.1.0";

    private static PulsarContainer pulsarContainer;

    @RegisterExtension static final KubeTestServer kubeServer = new KubeTestServer();

    private static PulsarAdmin admin;

    @Test
    public void testRunAITools() throws Exception {
        final String appId = "application";
        kubeServer.spyAgentCustomResources("tenant", appId + "-step1");
        final Map<String, Secret> secrets =
                kubeServer.spyAgentCustomResourcesSecrets("tenant", appId + "-step1");

        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
                                        "module.yaml",
                                        """
                                module: "module-1"
                                id: "pipeline-1"
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                  - name: "output-topic"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "drop-description"
                                    id: "step1"
                                    type: "drop-fields"
                                    input: "input-topic"
                                    output: "output-topic"
                                    configuration:
                                      fields:
                                        - "description"
                                """),
                                buildInstanceYaml(),
                                null)
                        .getApplication();

        @Cleanup
        NarFileHandler narFileHandler =
                new NarFileHandler(
                        AbstractApplicationRunner.agentsDirectory,
                        List.of(),
                        Thread.currentThread().getContextClassLoader());
        narFileHandler.scan();

        @Cleanup
        ApplicationDeployer deployer =
                ApplicationDeployer.builder()
                        .registry(new ClusterRuntimeRegistry())
                        .topicConnectionsRuntimeRegistry(
                                new TopicConnectionsRuntimeRegistry()
                                        .setPackageLoader(narFileHandler))
                        .pluginsRegistry(new PluginsRegistry())
                        .build();

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation(appId, applicationInstance);
        assertTrue(
                implementation.getConnectionImplementation(
                                module,
                                Connection.fromTopic(TopicDefinition.fromName("input-topic")))
                        instanceof Topic);
        deployer.setup("tenant", implementation);
        deployer.deploy("tenant", implementation, null);
        assertEquals(1, secrets.size());
        final Secret secret = secrets.values().iterator().next();
        final RuntimePodConfiguration runtimePodConfiguration =
                AgentResourcesFactory.readRuntimePodConfigurationFromSecret(secret);

        try (PulsarClient client =
                        PulsarClient.builder()
                                .serviceUrl(pulsarContainer.getPulsarBrokerUrl())
                                .build();
                Producer<byte[]> producer = client.newProducer().topic("input-topic").create();
                org.apache.pulsar.client.api.Consumer<byte[]> consumer =
                        client.newConsumer()
                                .topic("output-topic")
                                .subscriptionName("test")
                                .subscribe()) {

            // produce one message to the input-topic
            producer.newMessage()
                    .value(
                            "{\"name\": \"some name\", \"description\": \"some description\"}"
                                    .getBytes(StandardCharsets.UTF_8))
                    .key("key")
                    .properties(Map.of("header-key", "header-value"))
                    .send();
            producer.flush();

            AtomicInteger numLoops = new AtomicInteger();
            AgentRunner.runAgent(
                    runtimePodConfiguration,
                    null,
                    null,
                    AbstractApplicationRunner.agentsDirectory,
                    new AgentInfo(),
                    () -> numLoops.incrementAndGet() <= 5,
                    null,
                    false,
                    narFileHandler);

            // receive one message from the output-topic (written by the PodJavaRuntime)
            Message<byte[]> record = consumer.receive();
            assertEquals(
                    "{\"name\":\"some name\"}",
                    new String(record.getValue(), StandardCharsets.UTF_8));
            assertEquals("header-value", record.getProperties().get("header-key"));
        }
    }

    private static String buildInstanceYaml() {
        return """
                instance:
                  computeCluster:
                    type: "kubernetes"
                  streamingCluster:
                    type: "pulsar"
                    configuration:
                      admin:
                        serviceUrl: "%s"
                      service:
                        serviceUrl: "%s"
                      defaultTenant: "public"
                      defaultNamespace: "default"
                """
                .formatted(
                        "http://localhost:" + pulsarContainer.getMappedPort(8080),
                        "pulsar://localhost:" + pulsarContainer.getMappedPort(6650));
    }

    @BeforeAll
    public static void setup() throws Exception {
        pulsarContainer =
                new PulsarContainer(
                                DockerImageName.parse(IMAGE)
                                        .asCompatibleSubstituteFor("apachepulsar/pulsar"))
                        .withStartupTimeout(
                                Duration.ofSeconds(120)) // Mac M1 is slow with Intel docker images
                        .withLogConsumer(
                                outputFrame ->
                                        log.info("pulsar> {}", outputFrame.getUtf8String().trim()));
        // start Pulsar and wait for it to be ready to accept requests
        pulsarContainer.start();
        admin =
                PulsarAdmin.builder()
                        .serviceHttpUrl("http://localhost:" + pulsarContainer.getMappedPort(8080))
                        .build();

        try {
            admin.namespaces().createNamespace("public/default");
        } catch (PulsarAdminException.ConflictException exists) {
            // ignore
        }
    }

    @AfterAll
    public static void teardown() {
        if (admin != null) {
            admin.close();
        }
        if (pulsarContainer != null) {
            pulsarContainer.close();
        }
    }
}
