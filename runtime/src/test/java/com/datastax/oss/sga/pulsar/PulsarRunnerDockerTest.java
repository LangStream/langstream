package com.datastax.oss.sga.pulsar;

import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.Connection;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.TopicDefinition;
import com.datastax.oss.sga.api.runtime.ClusterRuntimeRegistry;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.PluginsRegistry;
import com.datastax.oss.sga.impl.deploy.ApplicationDeployer;
import com.datastax.oss.sga.impl.k8s.tests.KubeTestServer;
import com.datastax.oss.sga.impl.parser.ModelBuilder;
import com.datastax.oss.sga.runtime.agent.AgentSpec;
import com.datastax.oss.sga.runtime.agent.PodJavaRuntime;
import com.datastax.oss.sga.runtime.agent.RuntimePodConfiguration;
import com.datastax.oss.sga.runtime.impl.k8s.PodAgentConfiguration;
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
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class PulsarRunnerDockerTest {

    private static final String IMAGE = "datastax/lunastreaming-all:2.10_4.9";

    private static PulsarContainer pulsarContainer;

    @RegisterExtension
    static final KubeTestServer kubeServer = new KubeTestServer();

    private static PulsarAdmin admin;


    @Test
    public void testRunAITools() throws Exception {
        kubeServer.spyAgentCustomResources("tenant", "step1");

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
                                  - name: "drop-description"
                                    id: "step1"
                                    type: "drop-fields"
                                    input: "input-topic"
                                    output: "output-topic"
                                    configuration:
                                      fields:
                                        - "description"
                                """));

        ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        Module module = applicationInstance.getModule("module-1");

        final String appId = "application";
        ExecutionPlan implementation = deployer.createImplementation(appId, applicationInstance);
        assertTrue(implementation.getConnectionImplementation(module,
                new Connection(TopicDefinition.fromName("input-topic"))) instanceof PulsarTopic);

        List<PodAgentConfiguration> customResourceDefinitions = (List<PodAgentConfiguration>) deployer.deploy("tenant", implementation);
        Collection<String> topics = admin.topics().getList("public/default");
        log.info("Topics {}", topics);
        assertTrue(topics.contains("persistent://public/default/input-topic"));
        assertTrue(topics.contains("persistent://public/default/output-topic"));

        log.info("CRDS: {}", customResourceDefinitions);

        assertEquals(1, customResourceDefinitions.size());
        PodAgentConfiguration podAgentConfiguration = customResourceDefinitions.get(0);


        RuntimePodConfiguration runtimePodConfiguration = new RuntimePodConfiguration(
                podAgentConfiguration.input(),
                podAgentConfiguration.output(),
                new AgentSpec(AgentSpec.ComponentType.valueOf(
                        podAgentConfiguration.agentConfiguration().componentType()),
                        podAgentConfiguration.agentConfiguration().agentId(),
                        appId,
                        podAgentConfiguration.agentConfiguration().agentType(),
                        podAgentConfiguration.agentConfiguration().configuration()),
                applicationInstance.getInstance().streamingCluster()
        );

        try (PulsarClient client = PulsarClientUtils.buildPulsarClient(implementation.getApplication().getInstance().streamingCluster());
             Producer<byte[]> producer = client.newProducer().topic("input-topic").create();
             org.apache.pulsar.client.api.Consumer<byte[]> consumer = client.newConsumer().topic("output-topic").subscriptionName("test").subscribe();
             ) {

            // produce one message to the input-topic
            producer
                    .newMessage()
                    .value("{\"name\": \"some name\", \"description\": \"some description\"}".getBytes(StandardCharsets.UTF_8))
                    .key("key")
                    .properties(Map.of("header-key", "header-value"))
                    .send();
            producer.flush();

            PodJavaRuntime.run(runtimePodConfiguration, 5);

            // receive one message from the output-topic (written by the PodJavaRuntime)
            Message<byte[]> record = consumer.receive();
            assertEquals("{\"name\":\"some name\"}", new String(record.getValue(), StandardCharsets.UTF_8));
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
                """.formatted("http://localhost:" + pulsarContainer.getMappedPort(8080),
                "pulsar://localhost:" + pulsarContainer.getMappedPort(6650));
    }


    @BeforeAll
    public static void setup() throws Exception {
        pulsarContainer = new PulsarContainer(DockerImageName.parse(IMAGE)
                .asCompatibleSubstituteFor("apachepulsar/pulsar"))
                .withFunctionsWorker()
                .withStartupTimeout(Duration.ofSeconds(120)) // Mac M1 is slow with Intel docker images
                .withLogConsumer(new Consumer<OutputFrame>() {
                    @Override
                    public void accept(OutputFrame outputFrame) {
                        log.info("pulsar> {}", outputFrame.getUtf8String().trim());
                    }
                });
        // start Pulsar and wait for it to be ready to accept requests
        pulsarContainer.start();
        admin =
                PulsarAdmin.builder()
                        .serviceHttpUrl(
                                "http://localhost:" + pulsarContainer.getMappedPort(8080))
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