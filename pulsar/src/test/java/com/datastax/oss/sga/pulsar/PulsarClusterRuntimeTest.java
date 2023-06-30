package com.datastax.oss.sga.pulsar;

import com.datastax.oss.sga.api.model.ApplicationInstance;
import com.datastax.oss.sga.impl.RuntimeRegistry;
import com.datastax.oss.sga.impl.deploy.ApplicationDeployer;
import com.datastax.oss.sga.impl.parser.ModelBuilder;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

class PulsarClusterRuntimeTest {
    private static final String IMAGE = "datastax/lunastreaming:2.10_4.6";
    private static PulsarContainer pulsarContainer;
    private static PulsarAdmin admin;
    private static PulsarClient client;

    private static RuntimeRegistry runtimeRegistry = new RuntimeRegistry();


    @Test
    public void testDeployTopics() throws Exception {
        ApplicationInstance applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of("instance.yaml",
                        """
                                instance:
                                  streamingCluster:
                                    type: "pulsar"
                                    configuration:                                      
                                      webServiceUrl: "%s"
                                      defaultTenant: "public"
                                      defaultNamespace: "default"
                                """.formatted("http://localhost:" + pulsarContainer.getMappedPort(8080)),
                        "module.yaml", """
                                module: "module-1"
                                id: "pipeline-1"                                
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                    schema:
                                      type: avro
                                      schema: '{"type":"record","namespace":"examples","name":"Product","fields":[{"name":"id","type":"string"},{"name":"name","type":"string"},{"name":"description","type":"string"},{"name":"price","type":"double"},{"name":"category","type":"string"},{"name":"item-vector","type":"bytes"}]}}'
                                pipeline:
                                """));

        ApplicationDeployer<PulsarPhysicalApplicationInstance> deployer = ApplicationDeployer
                .<PulsarPhysicalApplicationInstance>builder()
                .registry(new RuntimeRegistry()).
                build();

        PulsarPhysicalApplicationInstance implementation = deployer.createImplementation(applicationInstance);
        deployer.deploy(applicationInstance, implementation);

        // verify that the topic exists
        admin.topics().getStats("public/default/input-topic");
    }

    @BeforeAll
    public static void setup() throws Exception {
        pulsarContainer = new PulsarContainer(DockerImageName.parse(IMAGE).asCompatibleSubstituteFor("apachepulsar/pulsar"));
        // start Pulsar and wait for it to be ready to accept requests
        pulsarContainer.start();
        admin =
                PulsarAdmin.builder()
                        .serviceHttpUrl(
                                "http://localhost:" + pulsarContainer.getMappedPort(8080))
                        .build();
        client =
                PulsarClient.builder()
                        .serviceUrl(
                                "pulsar://localhost:" + pulsarContainer.getMappedPort(6650))
                        .build();
    }

    @AfterAll
    public static void teardown() {
        if (client != null) {
            client.closeAsync();
        }
        if (admin != null) {
            admin.close();
        }
        if (pulsarContainer != null) {
            pulsarContainer.close();
        }
    }
}