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
package com.datastax.oss.sga.errors;

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
import com.datastax.oss.sga.runtime.agent.api.AgentInfo;
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
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class ErrorHandlingTest {

    private static KafkaContainer kafkaContainer;
    private static AdminClient admin;

    @RegisterExtension
    static final KubeTestServer kubeServer = new KubeTestServer();


    @Test
    public void testRunAITools() throws Exception {
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

        @Cleanup ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation("app", applicationInstance);
        assertTrue(implementation.getConnectionImplementation(module,
                Connection.from(TopicDefinition.fromName("input-topic"))) instanceof KafkaTopic);


        deployer.deploy(tenant, implementation, null);
        assertEquals(1, secrets.size());
        final Secret secret = secrets.values().iterator().next();
        final RuntimePodConfiguration runtimePodConfiguration =
                AgentResourcesFactory.readRuntimePodConfigurationFromSecret(secret);

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
                    "{\"name\": \"some name\", \"description\": \"some description\"}",
                    List.of(new RecordHeader("header-key", "header-value".getBytes(StandardCharsets.UTF_8)))))
                .get();
            producer.flush();

            AgentRunner.run(runtimePodConfiguration, null, null, new AgentInfo(), 5);

            // receive one message from the output-topic (written by the PodJavaRuntime)
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofSeconds(10));
            assertEquals(poll.count(), 1);
            ConsumerRecord<String, String> record = poll.iterator().next();
            assertEquals("{\"name\":\"some name\"}", record.value());
            assertEquals("header-value", new String(record.headers().lastHeader("header-key").value(), StandardCharsets.UTF_8));
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
