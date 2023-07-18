package com.datastax.oss.sga.runtime.impl.k8s;

import com.dastastax.oss.sga.kafka.runtime.KafkaTopic;
import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.TopicDefinition;
import com.datastax.oss.sga.api.runtime.AgentNode;
import com.datastax.oss.sga.api.runtime.ClusterRuntimeRegistry;
import com.datastax.oss.sga.api.runtime.Connection;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.PluginsRegistry;
import com.datastax.oss.sga.deployer.k8s.api.crds.agents.AgentCustomResource;
import com.datastax.oss.sga.impl.common.DefaultAgentNode;
import com.datastax.oss.sga.impl.deploy.ApplicationDeployer;
import com.datastax.oss.sga.impl.k8s.tests.KubeTestServer;
import com.datastax.oss.sga.impl.parser.ModelBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class KubernetesClusterRuntimeDockerTest {

    private static KafkaContainer kafkaContainer;
    @RegisterExtension
    static final KubeTestServer kubeServer = new KubeTestServer();

    @Test
    public void testMapGenericAgent() throws Exception {
        final String tenant = "tenant";
        final Map<String, AgentCustomResource> agentsCRs = kubeServer.spyAgentCustomResources("sga-tenant", "app-sink-1-id");
        Application applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of("instance.yaml",
                        buildInstanceYaml(),
                        "module.yaml", """
                                module: "module-1"
                                id: "pipeline-1"                                
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                    schema:
                                      type: avro
                                      schema: '{"type":"record","namespace":"examples","name":"Product","fields":[{"name":"id","type":"string"},{"name":"name","type":"string"},{"name":"description","type":"string"},{"name":"price","type":"double"},{"name":"category","type":"string"},{"name":"item_vector","type":"bytes"}]}}'
                                pipeline:
                                  - name: "sink1"
                                    id: "sink-1-id"
                                    type: "generic-agent"
                                    input: "input-topic"
                                    configuration:
                                      mappings: "id=value.id,name=value.name,description=value.description,item_vector=value.item_vector"
                                """));

        ApplicationDeployer deployer = getDeployer();

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation("app", applicationInstance);
        Connection connection = implementation.getConnectionImplementation(module,
                new com.datastax.oss.sga.api.model.Connection(TopicDefinition.fromName("input-topic")));
        assertNotNull(connection);

        AgentNode agentImplementation = implementation.getAgentImplementation(module, "sink-1-id");
        assertNotNull(agentImplementation);


        deployer.deploy(tenant, implementation, null);
        assertEquals(1, agentsCRs.size());
        final AgentCustomResource agent = agentsCRs.values().iterator().next();
        assertEquals("datastax/sga-generic-agent:latest", agent.getSpec().getImage());
        assertEquals("Never", agent.getSpec().getImagePullPolicy());
        assertEquals(tenant, agent.getSpec().getTenant());
        assertEquals("{\"input\":{\"auto.offset.reset\":\"earliest\",\"group.id\":\"sga-agent-sink-1-id\",\"key"
                + ".deserializer\":\"org.apache.kafka.common.serialization.StringDeserializer\","
                + "\"topic\":\"input-topic\",\"value.deserializer\":\"org.apache.kafka.common.serialization"
                + ".StringDeserializer\"},\"output\":{},\"agentConfiguration\":{\"agentId\":\"sink-1-id\","
                + "\"agentType\":\"generic-agent\",\"componentType\":\"FUNCTION\","
                + "\"configuration\":{\"mappings\":\"id=value.id,name=value.name,description=value.description,"
                + "item_vector=value.item_vector\"}},\"streamingCluster\":{\"type\":\"kafka\","
                + ("\"configuration\":{\"admin\":{\"bootstrap.servers\":\"PLAINTEXT://localhost:%d\"}}},"
                + "\"codeStorage\":{\"codeStorageArchiveId\":null}}").formatted(kafkaContainer.getFirstMappedPort()), agent.getSpec().getConfiguration());


    }

    private ApplicationDeployer getDeployer() {
        final KubernetesClusterRuntimeConfiguration config =
                new KubernetesClusterRuntimeConfiguration();
        config.setImage("datastax/sga-generic-agent:latest");
        config.setImagePullPolicy("Never");
        config.setNamespacePrefix("sga-");

        ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry(
                        Map.of("kubernetes", new ObjectMapper().convertValue(config, Map.class))))
                .pluginsRegistry(new PluginsRegistry())
                .build();
        return deployer;
    }

    @Test
    public void testOpenAIComputeEmbeddingFunction() throws Exception {
        final String tenant = "tenant";
        final Map<String, AgentCustomResource> agentsCRs = kubeServer.spyAgentCustomResources("sga-" + tenant, "app-step1");
        Application applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of("instance.yaml",
                        buildInstanceYaml(),
                        "configuration.yaml",
                        """
                                configuration:  
                                  resources:
                                    - name: open-ai
                                      type: open-ai-configuration
                                      configuration:
                                        url: "http://something"                                
                                        access-key: "xxcxcxc"
                                        provider: "azure"
                                  """,
                        "module.yaml", """
                                module: "module-1"
                                id: "pipeline-1"                                
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                    schema:
                                      type: avro
                                      schema: '{"type":"record","namespace":"examples","name":"Product","fields":[{"name":"id","type":"string"},{"name":"name","type":"string"},{"name":"description","type":"string"},{"name":"price","type":"double"},{"name":"category","type":"string"},{"name":"item_vector","type":"bytes"}]}}'
                                  - name: "output-topic"
                                    creation-mode: create-if-not-exists                                    
                                pipeline:
                                  - name: "compute-embeddings"
                                    id: "step1"
                                    type: "compute-ai-embeddings"
                                    input: "input-topic"
                                    output: "output-topic"
                                    configuration:                                      
                                      model: "text-embedding-ada-002"
                                      embeddings-field: "value.embeddings"
                                      text: "{{% value.name }} {{% value.description }}"
                                """));

        ApplicationDeployer deployer = getDeployer();

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation("app", applicationInstance);
        assertTrue(implementation.getConnectionImplementation(module,
                new com.datastax.oss.sga.api.model.Connection(
                        TopicDefinition.fromName("input-topic"))) instanceof KafkaTopic);
        assertTrue(implementation.getConnectionImplementation(module,
                new com.datastax.oss.sga.api.model.Connection(
                        TopicDefinition.fromName("output-topic"))) instanceof KafkaTopic);

        AgentNode agentImplementation = implementation.getAgentImplementation(module, "step1");
        assertNotNull(agentImplementation);
        DefaultAgentNode step =
                (DefaultAgentNode) agentImplementation;
        Map<String, Object> configuration = step.getConfiguration();
        log.info("Configuration: {}", configuration);
        Map<String, Object> openAIConfiguration = (Map<String, Object>) configuration.get("openai");
        log.info("openAIConfiguration: {}", openAIConfiguration);
        assertEquals("http://something", openAIConfiguration.get("url"));
        assertEquals("xxcxcxc", openAIConfiguration.get("access-key"));
        assertEquals("azure", openAIConfiguration.get("provider"));


        List<Map<String, Object>> steps = (List<Map<String, Object>>) configuration.get("steps");
        assertEquals(1, steps.size());
        Map<String, Object> step1 = steps.get(0);
        assertEquals("text-embedding-ada-002", step1.get("model"));
        assertEquals("value.embeddings", step1.get("embeddings-field"));
        assertEquals("{{ value.name }} {{ value.description }}", step1.get("text"));


        deployer.deploy(tenant, implementation, null);

        assertEquals(1, agentsCRs.size());
        final AgentCustomResource agent = agentsCRs.values().iterator().next();
        assertEquals("datastax/sga-generic-agent:latest", agent.getSpec().getImage());
        assertEquals("Never", agent.getSpec().getImagePullPolicy());
        assertEquals(tenant, agent.getSpec().getTenant());
        assertEquals(("{\"input\":{\"auto.offset.reset\":\"earliest\",\"group.id\":\"sga-agent-step1\",\"key"
                + ".deserializer\":\"org.apache.kafka.common.serialization.StringDeserializer\","
                + "\"topic\":\"input-topic\",\"value.deserializer\":\"org.apache.kafka.common.serialization"
                + ".StringDeserializer\"},\"output\":{\"key.serializer\":\"org.apache.kafka.common.serialization"
                + ".StringSerializer\",\"topic\":\"output-topic\",\"value.serializer\":\"org.apache.kafka.common"
                + ".serialization.StringSerializer\"},\"agentConfiguration\":{\"agentId\":\"step1\","
                + "\"agentType\":\"ai-tools\",\"componentType\":\"FUNCTION\","
                + "\"configuration\":{\"openai\":{\"access-key\":\"xxcxcxc\",\"provider\":\"azure\","
                + "\"url\":\"http://something\"},\"steps\":[{\"embeddings-field\":\"value.embeddings\","
                + "\"model\":\"text-embedding-ada-002\",\"text\":\"{{ value.name }} {{ value.description }}\","
                + "\"type\":\"compute-ai-embeddings\"}]}},\"streamingCluster\":{\"type\":\"kafka\","
                + "\"configuration\":{\"admin\":{\"bootstrap.servers\":\"PLAINTEXT://localhost:%d\"}}}}").formatted(kafkaContainer.getFirstMappedPort()), agent.getSpec().getConfiguration());


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
    }

    @AfterAll
    public static void teardown() {
        if (kafkaContainer != null) {
            kafkaContainer.close();
        }
    }

}