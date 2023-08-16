/**
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
package com.datastax.oss.sga.runtime.impl.k8s;

import com.dastastax.oss.sga.kafka.runtime.KafkaTopic;
import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.Connection;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.StreamingCluster;
import com.datastax.oss.sga.api.model.TopicDefinition;
import com.datastax.oss.sga.api.runtime.AgentNode;
import com.datastax.oss.sga.api.runtime.ClusterRuntimeRegistry;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.PluginsRegistry;
import com.datastax.oss.sga.deployer.k8s.agents.AgentResourcesFactory;
import com.datastax.oss.sga.deployer.k8s.api.crds.agents.AgentCustomResource;
import com.datastax.oss.sga.impl.common.DefaultAgentNode;
import com.datastax.oss.sga.impl.deploy.ApplicationDeployer;
import com.datastax.oss.sga.impl.k8s.tests.KubeTestServer;
import com.datastax.oss.sga.impl.parser.ModelBuilder;
import com.datastax.oss.sga.runtime.api.agent.AgentSpec;
import com.datastax.oss.sga.runtime.api.agent.RuntimePodConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
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

    private ApplicationDeployer getDeployer() {
        final KubernetesClusterRuntimeConfiguration config =
                new KubernetesClusterRuntimeConfiguration();
        config.setImage("datastax/sga-generic-agent:latest");
        config.setImagePullPolicy("Never");
        config.setNamespacePrefix("sga-");

        @Cleanup ApplicationDeployer deployer = ApplicationDeployer
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
        final Map<String, io.fabric8.kubernetes.api.model.Secret> secrets =
                kubeServer.spyAgentCustomResourcesSecrets("sga-" + tenant, "app-step1");
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

        @Cleanup ApplicationDeployer deployer = getDeployer();

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation("app", applicationInstance);
        assertTrue(implementation.getConnectionImplementation(module,
                Connection.fromTopic(
                        TopicDefinition.fromName("input-topic"))) instanceof KafkaTopic);
        assertTrue(implementation.getConnectionImplementation(module,
                Connection.fromTopic(
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

        assertEquals(tenant, agent.getSpec().getTenant());
        assertEquals("datastax/sga-generic-agent:latest", agent.getSpec().getImage());
        assertEquals("Never", agent.getSpec().getImagePullPolicy());
        assertEquals("step1", agent.getSpec().getAgentId());
        assertEquals("app", agent.getSpec().getApplicationId());
        assertEquals("app-step1", agent.getSpec().getAgentConfigSecretRef());

        assertEquals(1, secrets.size());
        final RuntimePodConfiguration runtimePodConfiguration =
                AgentResourcesFactory.readRuntimePodConfigurationFromSecret(secrets.values().iterator().next());
        assertEquals(Map.of("auto.offset.reset", "earliest",
                "group.id", "sga-agent-step1",
                "topic", "input-topic",
                "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"),
                runtimePodConfiguration.input());
        assertEquals(Map.of(
                "topic", "output-topic",
                "key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer",
                "value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer"
        ), runtimePodConfiguration.output());
        Map<String, Object> defaultErrorsAsMap = new HashMap<>();
        defaultErrorsAsMap.put("onFailure", "fail");
        defaultErrorsAsMap.put("retries", 0);
        assertEquals(new AgentSpec(AgentSpec.ComponentType.PROCESSOR, tenant, "step1", "app", "compute-ai-embeddings", Map.of(
                "steps", List.of(Map.of(
                        "type", "compute-ai-embeddings",
                        "model", "text-embedding-ada-002",
                        "embeddings-field", "value.embeddings",
                        "text", "{{ value.name }} {{ value.description }}"
                )),
                "openai", Map.of(
                        "url", "http://something",
                        "access-key", "xxcxcxc",
                        "provider", "azure"
                )
        ), defaultErrorsAsMap), runtimePodConfiguration.agent());
        assertEquals(new StreamingCluster("kafka", Map.of("admin", Map.of("bootstrap.servers", "PLAINTEXT://localhost:%d".formatted(kafkaContainer.getFirstMappedPort())))),
                runtimePodConfiguration.streamingCluster());
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
