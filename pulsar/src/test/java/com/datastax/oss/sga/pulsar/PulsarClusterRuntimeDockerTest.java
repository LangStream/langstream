package com.datastax.oss.sga.pulsar;

import com.datastax.oss.sga.api.model.ApplicationInstance;
import com.datastax.oss.sga.api.runtime.ClusterRuntimeRegistry;
import com.datastax.oss.sga.api.runtime.PhysicalApplicationInstance;
import com.datastax.oss.sga.api.runtime.PluginsRegistry;
import com.datastax.oss.sga.impl.deploy.ApplicationDeployer;
import com.datastax.oss.sga.impl.parser.ModelBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class PulsarClusterRuntimeDockerTest {
    private static final String IMAGE = "datastax/lunastreaming-all:2.10_4.6";
    private static PulsarContainer pulsarContainer;
    private static PulsarAdmin admin;
    private static PulsarClient client;

    @Test
    public void testDeployTopics() throws Exception {
        ApplicationInstance applicationInstance = ModelBuilder
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
                                """));

        ApplicationDeployer deployer = ApplicationDeployer
                .<PhysicalApplicationInstance>builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        PhysicalApplicationInstance implementation = deployer.createImplementation(applicationInstance);
        deployer.deploy(applicationInstance, implementation);

        // verify that the topic exists
        admin.topics().getStats("public/default/input-topic");
        // verify that the topic has a schema
        admin.schemas().getSchemaInfo("public/default/input-topic");
    }

    @Test
    public void testDeployCassandraSink() throws Exception {
        ApplicationInstance applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of("instance.yaml",
                        buildInstanceYaml(),
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
                                    input: "input-topic-cassandra"
                                    configuration:
                                      mappings: "id=value.id,name=value.name,description=value.description,item_vector=value.item_vector"
                                """));

        ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        PhysicalApplicationInstance implementation = deployer.createImplementation(applicationInstance);
        deployer.deploy(applicationInstance, implementation);

        // verify that the topic exists
        admin.topics().getStats("public/default/input-topic-cassandra");
        // verify that the topic has a schema
        admin.schemas().getSchemaInfo("public/default/input-topic-cassandra");

        // verify that we have the sink
        List<String> sinks = admin.sinks().listSinks("public", "default");
        assertTrue(sinks.contains("sink1"));
    }

    private static String buildInstanceYaml() {
        return """
                instance:
                  computeCluster:
                    type: "pulsar"
                    configuration:                                      
                      admin: 
                        serviceUrl: "%s"
                      defaultTenant: "public"
                      defaultNamespace: "default"
                  streamingCluster:
                    type: "pulsar"
                    configuration:                                      
                      admin: 
                        serviceUrl: "%s"
                      defaultTenant: "public"
                      defaultNamespace: "default"
                """.formatted("http://localhost:" + pulsarContainer.getMappedPort(8080),
                                     "http://localhost:" + pulsarContainer.getMappedPort(8080));
    }


    @Test
    public void testDeployDataGeneratorSource() throws Exception {
        ApplicationInstance applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of("instance.yaml",
                        buildInstanceYaml(),
                        "module.yaml", """
                                module: "module-1"
                                id: "pipeline-1"                                
                                topics:
                                  - name: "output-topic-from-file"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "source1"
                                    id: "source-1-id"
                                    type: "generic-pulsar-source"
                                    output: "output-topic-from-file"
                                    configuration:
                                      sourceType: "data-generator"
                                """));

        ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        PhysicalApplicationInstance implementation = deployer.createImplementation(applicationInstance);
        deployer.deploy(applicationInstance, implementation);

        // verify that the topic exists
        admin.topics().getStats("output-topic-from-file");

        // verify that we have the sink
        List<String> sources = admin.sources().listSources("public", "default");
        assertTrue(sources.contains("source1"));
    }

    @Test
    public void testDeployChainOfGenericFunctions() throws Exception {
        ApplicationInstance applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of("instance.yaml",
                        buildInstanceYaml(),
                        "module.yaml", """
                              module: "module-1"
                              id: "pipeline-1"
                              topics:
                                - name: "input-topic-fn"
                                  creation-mode: create-if-not-exists
                                - name: "output-topic-fn"
                                  creation-mode: create-if-not-exists
                              pipeline:
                                - name: "function1"
                                  id: "function-1-id"
                                  type: "generic-pulsar-function"
                                  input: "input-topic-fn"
                                  # the output is implicitly an intermediate topic
                                  configuration:
                                    functionType: "transforms"
                                    steps: []
                                - name: "function2"
                                  id: "function-2-id"
                                  type: "generic-pulsar-function"
                                  # the input is implicitly an intermediate topic
                                  output: "output-topic-fn"
                                  configuration:
                                    functionType: "transforms"
                                    steps: []
                                """));

        ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        PhysicalApplicationInstance implementation = deployer.createImplementation(applicationInstance);
        deployer.deploy(applicationInstance, implementation);

        // verify that the topics exist
        admin.topics().getStats("output-topic-fn");
        admin.topics().getStats("input-topic-fn");
        admin.topics().getStats("agent-function-1-id-output");


        // verify that we have the functions1
        List<String> functions = admin.functions().getFunctions("public", "default");
        assertTrue(functions.contains("function1"));
        assertTrue(functions.contains("function2"));
    }

    @BeforeAll
    public static void setup() throws Exception {
        pulsarContainer = new PulsarContainer(DockerImageName.parse(IMAGE)
                .asCompatibleSubstituteFor("apachepulsar/pulsar"))
                .withFunctionsWorker()
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
        client =
                PulsarClient.builder()
                        .serviceUrl(
                                "pulsar://localhost:" + pulsarContainer.getMappedPort(6650))
                        .build();


        try {
            admin.namespaces().createNamespace("public/default");
        } catch (PulsarAdminException.ConflictException exists) {
            // ignore
        }
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