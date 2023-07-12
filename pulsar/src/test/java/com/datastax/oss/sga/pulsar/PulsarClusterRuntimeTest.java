package com.datastax.oss.sga.pulsar;

import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.Connection;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.TopicDefinition;
import com.datastax.oss.sga.api.runtime.AgentNode;
import com.datastax.oss.sga.api.runtime.ClusterRuntimeRegistry;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.PluginsRegistry;
import com.datastax.oss.sga.impl.common.DefaultAgent;
import com.datastax.oss.sga.impl.deploy.ApplicationDeployer;
import com.datastax.oss.sga.impl.parser.ModelBuilder;
import com.datastax.oss.sga.pulsar.agents.AbstractPulsarAgentProvider;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class PulsarClusterRuntimeTest {

    @Test
    public void testMapCassandraSink() throws Exception {
        Application applicationInstance = ModelBuilder
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
                                    id: "sink-1-id"
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

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation(applicationInstance);
        assertTrue(implementation.getConnectionImplementation(module,
                new Connection(new TopicDefinition("input-topic-cassandra", null, null))) instanceof PulsarTopic);
        PulsarName pulsarName = new PulsarName("public", "default", "input-topic-cassandra");
        assertTrue(implementation.getTopics().values().stream().anyMatch( t-> ((PulsarTopic) t).name().equals(pulsarName)));

        AgentNode agentImplementation = implementation.getAgentImplementation(module, "sink-1-id");
        assertNotNull(agentImplementation);
        DefaultAgent genericSink =
                (DefaultAgent) agentImplementation;
        AbstractPulsarAgentProvider.PulsarAgentNodeMetadata pulsarSinkMetadata = genericSink.getCustomMetadata();
        assertEquals("cassandra-enhanced", pulsarSinkMetadata.getAgentType());
        assertEquals(new PulsarName("public", "default", "sink1"), pulsarSinkMetadata.getPulsarName());

    }

    @NotNull
    private static String buildInstanceYaml() {
        return """
                instance:
                  streamingCluster:
                    type: "pulsar"
                    configuration:
                      admin:                                      
                        serviceUrl: "http://localhost:8080"
                      defaultTenant: "public"
                      defaultNamespace: "default"
                  computeCluster:
                    type: "pulsar"
                    configuration:
                      admin:                                      
                        serviceUrl: "http://localhost:8080"
                      defaultTenant: "public"
                      defaultNamespace: "default"
                """;
    }

    @Test
    public void testMapGenericPulsarSink() throws Exception {
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
                                    type: "generic-pulsar-sink"
                                    input: "input-topic"
                                    configuration:
                                      sinkType: "some-sink-type-on-your-cluster"
                                      config1: "value"
                                      config2: "value2"
                                """));

        ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation(applicationInstance);
        assertTrue(implementation.getConnectionImplementation(module,
                new Connection(new TopicDefinition("input-topic", null, null))) instanceof PulsarTopic);
        PulsarName pulsarName = new PulsarName("public", "default", "input-topic");
        assertTrue(implementation.getTopics().values().stream().anyMatch( t-> ((PulsarTopic) t).name().equals(pulsarName)));

        AgentNode agentImplementation = implementation.getAgentImplementation(module, "sink-1-id");
        assertNotNull(agentImplementation);
        DefaultAgent genericSink =
                (DefaultAgent) agentImplementation;
        AbstractPulsarAgentProvider.PulsarAgentNodeMetadata pulsarSinkMetadata = genericSink.getCustomMetadata();
        assertEquals("some-sink-type-on-your-cluster", pulsarSinkMetadata.getAgentType());
        assertEquals(new PulsarName("public", "default", "sink1"), pulsarSinkMetadata.getPulsarName());

    }

    @Test
    public void testMapGenericPulsarSource() throws Exception {
        Application applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of("instance.yaml",
                        buildInstanceYaml(),
                        "module.yaml", """
                                module: "module-1"
                                id: "pipeline-1"                                
                                topics:
                                  - name: "output-topic"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "source1"
                                    id: "source-1-id"
                                    type: "generic-pulsar-source"
                                    output: "output-topic"
                                    configuration:
                                      sourceType: "some-source-type-on-your-cluster"
                                      config1: "value"
                                      config2: "value2"
                                """));

        ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation(applicationInstance);
        assertTrue(implementation.getConnectionImplementation(module,
                new Connection(new TopicDefinition("output-topic", null, null))) instanceof PulsarTopic);
        PulsarName pulsarName = new PulsarName("public", "default", "output-topic");
        assertTrue(implementation.getTopics().values().stream().anyMatch( t-> ((PulsarTopic) t).name().equals(pulsarName)));

        AgentNode agentImplementation = implementation.getAgentImplementation(module, "source-1-id");
        assertNotNull(agentImplementation);
        DefaultAgent genericSink =
                (DefaultAgent) agentImplementation;
        AbstractPulsarAgentProvider.PulsarAgentNodeMetadata pulsarSourceMetadata = genericSink.getCustomMetadata();
        assertEquals("some-source-type-on-your-cluster", pulsarSourceMetadata.getAgentType());
        assertEquals(new PulsarName("public", "default", "source1"), pulsarSourceMetadata.getPulsarName());

    }


    @Test
    public void testMapGenericPulsarFunction() throws Exception {
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
                                  - name: "function1"
                                    id: "function-1-id"
                                    type: "generic-pulsar-function"
                                    input: "input-topic"
                                    output: "output-topic"
                                    configuration:
                                      functionType: "some-function-type-on-your-cluster"
                                      functionClassname: "a.b.c.ClassName"
                                      config1: "value"
                                      config2: "value2"
                                """));

        ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation(applicationInstance);
        {
            assertTrue(implementation.getConnectionImplementation(module,
                    new Connection(new TopicDefinition("input-topic", null, null))) instanceof PulsarTopic);
            PulsarName pulsarName = new PulsarName("public", "default", "input-topic");
            assertTrue(implementation.getTopics().values().stream().anyMatch( t-> ((PulsarTopic) t).name().equals(pulsarName)));
        }
        {
            assertTrue(implementation.getConnectionImplementation(module,
                    new Connection(new TopicDefinition("output-topic", null, null))) instanceof PulsarTopic);
            PulsarName pulsarName = new PulsarName("public", "default", "output-topic");
            assertTrue(implementation.getTopics().values().stream().anyMatch( t-> ((PulsarTopic) t).name().equals(pulsarName)));
        }

        AgentNode agentImplementation = implementation.getAgentImplementation(module, "function-1-id");
        assertNotNull(agentImplementation);
        DefaultAgent genericSink =
                (DefaultAgent) agentImplementation;
        AbstractPulsarAgentProvider.PulsarAgentNodeMetadata pulsarSourceMetadata =
                genericSink.getCustomMetadata();
        assertEquals("some-function-type-on-your-cluster", pulsarSourceMetadata.getAgentType());
        assertEquals(new PulsarName("public", "default", "function1"), pulsarSourceMetadata.getPulsarName());

    }


    @Test
    public void testMapGenericPulsarFunctionsChain() throws Exception {
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
                                  - name: "function1"
                                    id: "function-1-id"
                                    type: "generic-pulsar-function"
                                    input: "input-topic"
                                    # the output is implicitly an intermediate topic                                    
                                    configuration:
                                      functionType: "some-function-type-on-your-cluster"
                                      functionClassname: "a.b.c.ClassName"
                                      config1: "value"
                                      config2: "value2"
                                  - name: "function2"
                                    id: "function-2-id"
                                    type: "generic-pulsar-function"
                                    # the input is implicitly an intermediate topic                                    
                                    output: "output-topic"
                                    configuration:
                                      functionType: "some-function-type-on-your-cluster"
                                      functionClassname: "a.b.c.ClassName"
                                      config1: "value"
                                      config2: "value2"
                                """));

        ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation(applicationInstance);
        {
            assertTrue(implementation.getConnectionImplementation(module,
                    new Connection(new TopicDefinition("input-topic", null, null))) instanceof PulsarTopic);
            PulsarName pulsarName = new PulsarName("public", "default", "input-topic");
            assertTrue(implementation.getTopics().values().stream().anyMatch( t-> ((PulsarTopic) t).name().equals(pulsarName)));
        }
        {
            assertTrue(implementation.getConnectionImplementation(module,
                    new Connection(new TopicDefinition("output-topic", null, null))) instanceof PulsarTopic);
            PulsarName pulsarName = new PulsarName("public", "default", "output-topic");
            assertTrue(implementation.getTopics().values().stream().anyMatch( t-> ((PulsarTopic) t).name().equals(pulsarName)));
        }

        {
            assertTrue(implementation.getConnectionImplementation(module, new Connection(
                    new TopicDefinition("agent-function-1-id-output", null, null))) instanceof PulsarTopic);
            PulsarName pulsarName = new PulsarName("public", "default", "agent-function-1-id-output");
            assertTrue(implementation.getTopics().values().stream().anyMatch( t-> ((PulsarTopic) t).name().equals(pulsarName)));
        }


        assertEquals(3, implementation.getTopics().size());

        {
            AgentNode agentImplementation = implementation.getAgentImplementation(module, "function-1-id");
            assertNotNull(agentImplementation);
            DefaultAgent genericSink =
                    (DefaultAgent) agentImplementation;
            AbstractPulsarAgentProvider.PulsarAgentNodeMetadata pulsarSourceMetadata =
                    genericSink.getCustomMetadata();
            assertEquals("some-function-type-on-your-cluster", pulsarSourceMetadata.getAgentType());
            assertEquals(new PulsarName("public", "default", "function1"), pulsarSourceMetadata.getPulsarName());
        }

        {
            AgentNode agentImplementation = implementation.getAgentImplementation(module, "function-2-id");
            assertNotNull(agentImplementation);
            DefaultAgent genericSink =
                    (DefaultAgent) agentImplementation;
            AbstractPulsarAgentProvider.PulsarAgentNodeMetadata pulsarSourceMetadata =
                    genericSink.getCustomMetadata();
            assertEquals("some-function-type-on-your-cluster", pulsarSourceMetadata.getAgentType());
            assertEquals(new PulsarName("public", "default", "function2"), pulsarSourceMetadata.getPulsarName());
        }

    }


    @Test
    public void testOpenAIComputeEmbeddingFunction() throws Exception {
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

        ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation(applicationInstance);
        assertTrue(implementation.getConnectionImplementation(module,
                new Connection(new TopicDefinition("input-topic", null, null))) instanceof PulsarTopic);
        assertTrue(implementation.getConnectionImplementation(module,
                new Connection(new TopicDefinition("output-topic", null, null))) instanceof PulsarTopic);

        AgentNode agentImplementation = implementation.getAgentImplementation(module, "step1");
        assertNotNull(agentImplementation);
        DefaultAgent step =
                (DefaultAgent) agentImplementation;
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


    }

    @Test
    public void testSanitizePipelineName() throws Exception {
        Application applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of("instance.yaml",
                        buildInstanceYaml(),
                        "module.yaml", """
                                module: "module-1"
                                id: "pipeline-1"                                
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "My function name with spaces"
                                    id: "step1"
                                    type: "compute-ai-embeddings"
                                    input: "input-topic"
                                    output: "input-topic"
                                    configuration:                                      
                                      model: "text-embedding-ada-002"
                                      embeddings-field: "value.embeddings"
                                      text: "{{% value.name }} {{% value.description }}"
                                  - name: "My sink name with spaces"
                                    id: "sink1"
                                    type: "generic-pulsar-sink"
                                    input: "input-topic"
                                    configuration:
                                      sinkType: "some-sink-type-on-your-cluster"
                                      config1: "value"
                                      config2: "value2"
                                  - name: "My source name with spaces"
                                    id: "source1"
                                    type: "generic-pulsar-source"
                                    output: "input-topic"
                                    configuration:
                                      sourceType: "some-source-type-on-your-cluster"
                                      config1: "value"
                                      config2: "value2"
                                """));

        ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation(applicationInstance);
        final DefaultAgent functionPhysicalImpl =
                (DefaultAgent) implementation.getAgentImplementation(module,
                        "step1");
        assertEquals("my-function-name-with-spaces",
                ((AbstractPulsarAgentProvider.PulsarAgentNodeMetadata) functionPhysicalImpl.getCustomMetadata()).getPulsarName()
                        .name());

        final DefaultAgent sinkPhysicalImpl =
                (DefaultAgent) implementation.getAgentImplementation(module,
                        "sink1");
        assertEquals("my-sink-name-with-spaces",
                ((AbstractPulsarAgentProvider.PulsarAgentNodeMetadata) sinkPhysicalImpl.getCustomMetadata()).getPulsarName()
                        .name());

        final DefaultAgent sourcePhysicalImpl =
                (DefaultAgent) implementation.getAgentImplementation(module,
                        "source1");
        assertEquals("my-source-name-with-spaces",
                ((AbstractPulsarAgentProvider.PulsarAgentNodeMetadata) sourcePhysicalImpl.getCustomMetadata()).getPulsarName()
                        .name());
    }


    @Test
    public void testMergeGenAIToolKitAgents() throws Exception {
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
                                    configuration:                                      
                                      model: "text-embedding-ada-002"
                                      embeddings-field: "value.embeddings"
                                      text: "{{% value.name }} {{% value.description }}"
                                  - name: "compute-embeddings"
                                    id: "step2"
                                    type: "compute-ai-embeddings"                                    
                                    output: "output-topic"
                                    configuration:                                      
                                      model: "text-embedding-ada-003"
                                      embeddings-field: "value.embeddings2"
                                      text: "{{% value.name }}"
                                """));

        ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation(applicationInstance);

        AgentNode agentImplementation = implementation.getAgentImplementation(module, "step1");
        assertNotNull(agentImplementation);
        DefaultAgent step =
                (DefaultAgent) agentImplementation;
        Map<String, Object> configuration = step.getConfiguration();
        log.info("Configuration: {}", configuration);
        Map<String, Object> openAIConfiguration = (Map<String, Object>) configuration.get("openai");
        log.info("openAIConfiguration: {}", openAIConfiguration);
        assertEquals("http://something", openAIConfiguration.get("url"));
        assertEquals("xxcxcxc", openAIConfiguration.get("access-key"));
        assertEquals("azure", openAIConfiguration.get("provider"));


        List<Map<String, Object>> steps = (List<Map<String, Object>>) configuration.get("steps");
        assertEquals(2, steps.size());
        Map<String, Object> step1 = steps.get(0);
        assertEquals("text-embedding-ada-002", step1.get("model"));
        assertEquals("value.embeddings", step1.get("embeddings-field"));
        assertEquals("{{ value.name }} {{ value.description }}", step1.get("text"));

        Map<String, Object> step2 = steps.get(1);
        assertEquals("text-embedding-ada-003", step2.get("model"));
        assertEquals("value.embeddings2", step2.get("embeddings-field"));
        assertEquals("{{ value.name }}", step2.get("text"));


        // verify that the intermediate topic is not created
        log.info("topics {}", implementation.getTopics());
        assertTrue(implementation.getConnectionImplementation(module,
                new Connection(new TopicDefinition("input-topic", null, null))) instanceof PulsarTopic);
        assertTrue(implementation.getConnectionImplementation(module,
                new Connection(new TopicDefinition("output-topic", null, null))) instanceof PulsarTopic);
        assertEquals(2, implementation.getTopics().size());

    }

}