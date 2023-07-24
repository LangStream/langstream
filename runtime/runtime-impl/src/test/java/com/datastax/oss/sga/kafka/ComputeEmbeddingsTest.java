package com.datastax.oss.sga.kafka;

import com.dastastax.oss.sga.kafka.runtime.KafkaTopic;
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
import com.datastax.oss.sga.runtime.agent.AgentRunner;
import com.datastax.oss.sga.runtime.api.agent.AgentSpec;
import com.datastax.oss.sga.runtime.api.agent.CodeStorageConfig;
import com.datastax.oss.sga.runtime.api.agent.RuntimePodConfiguration;
import com.datastax.oss.sga.runtime.k8s.api.PodAgentConfiguration;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.utility.DockerImageName;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@WireMockTest
class ComputeEmbeddingsTest {

    private static KafkaContainer kafkaContainer;
    private static AdminClient admin;

    @RegisterExtension
    static final KubeTestServer kubeServer = new KubeTestServer();

    @AllArgsConstructor
    private static class EmbeddingsConfig {
        String model;
        String providerConfiguration;
        Runnable stubMakers;

        @Override
        public String toString() {
            return "EmbeddingsConfig{" +
                    "model='" + model + '\'' +
                    '}';
        }
    }


    static WireMockRuntimeInfo wireMockRuntimeInfo;

    @BeforeAll
    static void onBeforeAll(WireMockRuntimeInfo info){
        wireMockRuntimeInfo = info;
    }


    private static Stream<Arguments> providers() {
        return Stream.of(
                Arguments.of(
                        new EmbeddingsConfig("textembedding-gecko", """
                             configuration:
                                 resources:
                                    - type: "vertex-configuration"
                                      name: "Vertex configuration"
                                      configuration:                                        
                                        url: "%s"     
                                        region: "us-east1"
                                        project: "the-project"
                                        token: "some-token"
                        """.formatted(wireMockRuntimeInfo.getHttpBaseUrl()),
                                () -> {
                                    stubFor(post("/v1/projects/the-project/locations/us-east1/publishers/google/models/textembedding-gecko:predict")
                                            .willReturn(okJson(""" 
                                               {
                                                  "predictions": [
                                                    {
                                                      "embeddings": {
                                                        "statistics": {
                                                          "truncated": false,
                                                          "token_count": 6
                                                        },
                                                        "values": [ 1.0, 5.4, 8.7]
                                                      }
                                                    }
                                                  ]
                                                }
                """)));
                                })),
                Arguments.of(new EmbeddingsConfig("some-model", """
                             configuration:
                                 resources:
                                    - type: "hugging-face-configuration"
                                      name: "Hugging Face API configuration"
                                      configuration:                                        
                                        api-url: "%s"
                                        model-check-url: "%s"                                             
                                        access-key: "some-token"
                                        provider: "api"
                        """.formatted(wireMockRuntimeInfo.getHttpBaseUrl()+"/embeddings/",
                        wireMockRuntimeInfo.getHttpBaseUrl()+"/modelcheck/"),
                                () -> {
                                    stubFor(get("/modelcheck/some-model")
                                            .willReturn(okJson("{\"modelId\": \"some-model\",\"tags\": [\"sentence-transformers\"]}")));
                                    stubFor(post("/embeddings/some-model")
                                            .willReturn(okJson("[[1.0, 5.4, 8.7]]")));
                                    }
                )));
    }



    @ParameterizedTest
    @MethodSource("providers")
    public void testComputeEmbeddings(EmbeddingsConfig config) throws Exception {
        config.stubMakers.run();
        String tenant = "tenant";
        kubeServer.spyAgentCustomResources(tenant, "app-step1");

        Application applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of(
                        "configuration.yaml",
                        config.providerConfiguration,
                        "instance.yaml",
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
                                  - name: "compute-embeddings"
                                    id: "step1"
                                    type: "compute-ai-embeddings"
                                    input: "input-topic"
                                    output: "output-topic"
                                    configuration:                                      
                                      model: "%s"
                                      embeddings-field: "value.embeddings"
                                      text: "something to embed"
                                """.formatted(config.model)));

        ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation("app", applicationInstance);
        assertTrue(implementation.getConnectionImplementation(module,
                new Connection(TopicDefinition.fromName("input-topic"))) instanceof KafkaTopic);

        List<PodAgentConfiguration> customResourceDefinitions = (List<PodAgentConfiguration>) deployer.deploy(tenant, implementation, null);

        Set<String> topics = admin.listTopics().names().get();
        log.info("Topics {}", topics);
        assertTrue(topics.contains("input-topic"));

        log.info("CRDS: {}", customResourceDefinitions);

        assertEquals(1, customResourceDefinitions.size());
        PodAgentConfiguration podAgentConfiguration = customResourceDefinitions.get(0);

        RuntimePodConfiguration runtimePodConfiguration = new RuntimePodConfiguration(
                podAgentConfiguration.input(),
                podAgentConfiguration.output(),
                new AgentSpec(AgentSpec.ComponentType.valueOf(
                        podAgentConfiguration.agentConfiguration().componentType()),
                        tenant,
                        podAgentConfiguration.agentConfiguration().agentId(),
                        "application",
                        podAgentConfiguration.agentConfiguration().agentType(),
                        podAgentConfiguration.agentConfiguration().configuration()),
                applicationInstance.getInstance().streamingCluster(),
                new CodeStorageConfig("none", "none", Map.of())
        );

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


            // produce one message to the input-topic
            producer
                .send(new ProducerRecord<>(
                    "input-topic",
                    null,
                    "key",
                    "{\"name\": \"some name\", \"description\": \"some description\"}"))
                .get();
            producer.flush();

            AgentRunner.run(runtimePodConfiguration, null, null, 5);

            // receive one message from the output-topic (written by the PodJavaRuntime)
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofSeconds(10));
            assertEquals(poll.count(), 1);
            ConsumerRecord<String, String> record = poll.iterator().next();
            assertEquals("{\"name\":\"some name\",\"description\":\"some description\",\"embeddings\":[1.0,5.4,8.7]}", record.value());
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