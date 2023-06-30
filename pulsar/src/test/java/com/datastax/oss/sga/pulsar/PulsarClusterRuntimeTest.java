package com.datastax.oss.sga.pulsar;

import com.datastax.oss.sga.api.model.ApplicationInstance;
import com.datastax.oss.sga.api.runtime.ClusterRuntimeRegistry;
import com.datastax.oss.sga.api.runtime.PluginsRegistry;
import com.datastax.oss.sga.impl.deploy.ApplicationDeployer;
import com.datastax.oss.sga.impl.parser.ModelBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class PulsarClusterRuntimeTest {
    private static final String IMAGE = "datastax/lunastreaming-all:2.10_4.6";
    private static PulsarContainer pulsarContainer;
    private static PulsarAdmin admin;
    private static PulsarClient client;

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
                                      schema: '{"type":"record","namespace":"examples","name":"Product","fields":[{"name":"id","type":"string"},{"name":"name","type":"string"},{"name":"description","type":"string"},{"name":"price","type":"double"},{"name":"category","type":"string"},{"name":"item_vector","type":"bytes"}]}}'
                                pipeline:
                                """));

        ApplicationDeployer<PulsarPhysicalApplicationInstance> deployer = ApplicationDeployer
                .<PulsarPhysicalApplicationInstance>builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        PulsarPhysicalApplicationInstance implementation = deployer.createImplementation(applicationInstance);
        deployer.deploy(applicationInstance, implementation);

        // verify that the topic exists
        admin.topics().getStats("public/default/input-topic");
        // verify that the topic has a schema
        admin.schemas().getSchemaInfo("public/default/input-topic");
    }

    @Test
    @Disabled
    public void testDeployCassandraSink() throws Exception {
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
                                  - name: "input-topic-cassandra"
                                    creation-mode: create-if-not-exists
                                    schema:
                                      type: avro
                                      schema: '{"type":"record","namespace":"examples","name":"Product","fields":[{"name":"id","type":"string"},{"name":"name","type":"string"},{"name":"description","type":"string"},{"name":"price","type":"double"},{"name":"category","type":"string"},{"name":"item_vector","type":"bytes"}]}}'
                                pipeline:
                                  - name: "sink1"
                                    type: "cassandra-sink"
                                    configuration:
                                      mappings: "id=value.id,name=value.name,description=value.description,item_vector=value.item_vector"
                                """));

        ApplicationDeployer<PulsarPhysicalApplicationInstance> deployer = ApplicationDeployer
                .<PulsarPhysicalApplicationInstance>builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        PulsarPhysicalApplicationInstance implementation = deployer.createImplementation(applicationInstance);
        deployer.deploy(applicationInstance, implementation);

        // verify that the topic exists
        admin.topics().getStats("public/default/input-topic");
        // verify that the topic has a schema
        admin.schemas().getSchemaInfo("public/default/input-topic");

        // verify that we have the sink
        List<String> sinks = admin.sinks().listSinks("public", "default");
        assertTrue(sinks.contains("sink1"));
    }

    @BeforeAll
    public static void setup() throws Exception {
        pulsarContainer = new PulsarContainer(DockerImageName.parse(IMAGE)
                .asCompatibleSubstituteFor("apachepulsar/pulsar"))
                .withFunctionsWorker();
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