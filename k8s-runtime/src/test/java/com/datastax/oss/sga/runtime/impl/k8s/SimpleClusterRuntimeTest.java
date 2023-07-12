package com.datastax.oss.sga.runtime.impl.k8s;

import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.Connection;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.TopicDefinition;
import com.datastax.oss.sga.api.runtime.AgentNode;
import com.datastax.oss.sga.api.runtime.ClusterRuntimeRegistry;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.PluginsRegistry;
import com.datastax.oss.sga.impl.common.DefaultAgentNode;
import com.datastax.oss.sga.impl.deploy.ApplicationDeployer;
import com.datastax.oss.sga.impl.noop.NoOpStreamingClusterRuntimeProvider;
import com.datastax.oss.sga.impl.parser.ModelBuilder;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class SimpleClusterRuntimeTest {

    @NotNull
    private static String buildInstanceYaml() {
        return """
                instance:
                  streamingCluster:
                    type: "noop"                    
                  computeCluster:
                    type: "kubernetes"                    
                """;
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
                                  - name: "generic-agent"
                                    id: "function-1-id"
                                    type: "generic-agent"
                                    input: "input-topic"
                                    # the output is implicitly an intermediate topic                                    
                                    configuration:
                                      config1: "value"
                                      config2: "value2"
                                  - name: "function2"
                                    id: "function-2-id"
                                    type: "generic-agent"
                                    # the input is implicitly an intermediate topic                                    
                                    output: "output-topic"
                                    configuration:
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
                    new Connection(new TopicDefinition("input-topic", null, null))) instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);
            assertTrue(implementation.getTopics().values().stream().anyMatch( t-> ((NoOpStreamingClusterRuntimeProvider.SimpleTopic) t).name().equals("input-topic")));
        }
        {
            assertTrue(implementation.getConnectionImplementation(module,
                    new Connection(new TopicDefinition("output-topic", null, null))) instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);
            assertTrue(implementation.getTopics().values().stream().anyMatch( t-> ((NoOpStreamingClusterRuntimeProvider.SimpleTopic) t).name().equals("output-topic")));
        }

        {
            assertTrue(implementation.getConnectionImplementation(module, new Connection(
                    new TopicDefinition("agent-function-1-id-output", null, null))) instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);
            assertTrue(implementation.getTopics().values().stream().anyMatch( t-> ((NoOpStreamingClusterRuntimeProvider.SimpleTopic) t).name().equals("agent-function-1-id-output")));
        }


        assertEquals(3, implementation.getTopics().size());

        {
            AgentNode agentImplementation = implementation.getAgentImplementation(module, "function-1-id");
            assertNotNull(agentImplementation);
            assertEquals("generic-agent", agentImplementation.getAgentType());
            assertEquals("function-1-id", agentImplementation.getId());
        }

        {
            AgentNode agentImplementation = implementation.getAgentImplementation(module, "function-2-id");
            assertNotNull(agentImplementation);
            assertEquals("generic-agent", agentImplementation.getAgentType());
            assertEquals("function-2-id", agentImplementation.getId());
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
                new Connection(new TopicDefinition("input-topic", null, null))) instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);
        assertTrue(implementation.getConnectionImplementation(module,
                new Connection(new TopicDefinition("output-topic", null, null))) instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);

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
                new Connection(new TopicDefinition("input-topic", null, null))) instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);
        assertTrue(implementation.getConnectionImplementation(module,
                new Connection(new TopicDefinition("output-topic", null, null))) instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);
        assertEquals(2, implementation.getTopics().size());

    }

}