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
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class GenAIAgentsTest {

    @NotNull
    private static String buildInstanceYaml() {
        return """
                instance:
                  streamingCluster:
                    type: "noop"                    
                  computeCluster:
                    type: "none"                    
                """;
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

        try (ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();) {

            Module module = applicationInstance.getModule("module-1");

            ExecutionPlan implementation = deployer.createImplementation("app", applicationInstance);
            assertEquals(1, implementation.getAgents().size());
            assertTrue(implementation.getConnectionImplementation(module,
                    Connection.from(TopicDefinition.fromName(
                            "input-topic"))) instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);
            assertTrue(implementation.getConnectionImplementation(module,
                    Connection.from(TopicDefinition.fromName(
                            "output-topic"))) instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);

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
                                  - name: "drop"
                                    id: "step2"
                                    type: "drop-fields"                                    
                                    output: "output-topic"
                                    configuration:                                      
                                      fields: "embeddings"
                                      part: "value"
                                """));

        @Cleanup ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation("app", applicationInstance);
        log.info("Agents: {}", implementation.getAgents());
        assertEquals(1, implementation.getAgents().size());

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
        assertEquals("compute-ai-embeddings", step1.get("type"));
        assertEquals("text-embedding-ada-002", step1.get("model"));
        assertEquals("value.embeddings", step1.get("embeddings-field"));
        assertEquals("{{ value.name }} {{ value.description }}", step1.get("text"));

        Map<String, Object> step2 = steps.get(1);
        assertEquals("drop-fields", step2.get("type"));
        assertEquals("embeddings", step2.get("fields"));
        assertEquals("value", step2.get("part"));


        // verify that the intermediate topic is not created
        log.info("topics {}", implementation.getTopics().keySet().stream().map(TopicDefinition::getName).toList());
        assertTrue(implementation.getConnectionImplementation(module,
                Connection.from(TopicDefinition.fromName("input-topic"))) instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);
        assertTrue(implementation.getConnectionImplementation(module,
                Connection.from(TopicDefinition.fromName("output-topic"))) instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);
        assertEquals(2, implementation.getTopics().size());

    }


    @Test
    public void testMapAllGenAIToolKitAgents() throws Exception {
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
                                    - name: my-database
                                      type: datasource
                                      configuration:
                                        connectionUrl: localhost:1544    
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
                                    id: step1
                                    type: "compute-ai-embeddings"
                                    input: "input-topic"                                    
                                    configuration:                                      
                                      model: "text-embedding-ada-002"
                                      embeddings-field: "value.embeddings"
                                      text: "{{% value.name }} {{% value.description }}"
                                  - name: "dropfields"
                                    type: "drop-fields"                                    
                                    configuration:                                      
                                      fields: "embeddings"
                                      part: "value"
                                  - name: "drop"
                                    type: "drop"                                    
                                    configuration:                                      
                                      when: "true"
                                  - name: "query"
                                    type: "query"                                    
                                    configuration:
                                      datasource: "my-database"         
                                      query: "select * from table"
                                      output-field: "value.queryresult"                             
                                      fields:
                                        - "value.field1"                                          
                                        - "key.field2"
                                  - name: "unwrap-key-value"
                                    type: "unwrap-key-value"
                                  - name: "flatten"
                                    type: "flatten"
                                  - name: "compute"
                                    type: "compute"
                                    configuration:
                                       fields:
                                        - name: "field1"
                                          type: "string"
                                          expression: "value.field1"
                                        - name: "field2"
                                          type: "string"
                                          expression: "value.field2"                                         
                                  - name: "merge-key-value"
                                    type: "merge-key-value"
                                  - name: "ai-chat-completions"
                                    type: "ai-chat-completions"
                                    configuration:   
                                      model: "davinci"                                   
                                      completion-field: "value.chatresult"
                                      messages:
                                         - role: user
                                           content: xxx
                                  - name: "casttojson"
                                    type: "cast"                                    
                                    output: "output-topic"
                                    configuration:                                      
                                      schema-type: "string"
                                """));

        @Cleanup ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation("app", applicationInstance);
        log.info("Agents: {}", implementation.getAgents());
        assertEquals(1, implementation.getAgents().size());

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
        assertEquals(10, steps.size());

        // verify that the intermediate topics are not created
        log.info("topics {}", implementation.getTopics());
        assertEquals(2, implementation.getTopics().size());
        assertTrue(implementation.getConnectionImplementation(module,
                Connection.from(TopicDefinition.fromName("input-topic"))) instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);
        assertTrue(implementation.getConnectionImplementation(module,
                Connection.from(TopicDefinition.fromName("output-topic"))) instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);


    }


    @Test
    public void testMultipleQuerySteps() throws Exception {
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
                                    - name: my-database-1
                                      type: datasource
                                      configuration:
                                        connectionUrl: localhost:1544
                                    - name: my-database-2
                                      type: datasource
                                      configuration:
                                        connectionUrl: localhost:1545
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
                                  - name: "query1"
                                    id: query1
                                    type: "query"                                    
                                    configuration:
                                      datasource: "my-database-1"         
                                      query: "select * from table"
                                      output-field: "value.queryresult"                             
                                      fields:
                                        - "value.field1"                                          
                                        - "key.field2"
                                  - name: "query2"
                                    id: query2
                                    type: "query"                                    
                                    configuration:
                                      datasource: "my-database-2"         
                                      query: "select * from table2"
                                      output-field: "value.queryresult2"                             
                                      fields:
                                        - "value.field1"                                          
                                        - "key.field2"
                                  - name: "casttojson"
                                    type: "cast"                                    
                                    output: "output-topic"
                                    configuration:                                      
                                      schema-type: "string"
                                """));

        @Cleanup ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation("app", applicationInstance);
        assertEquals(2, implementation.getAgents().size());

        {
            AgentNode agentImplementation = implementation.getAgentImplementation(module, "query1");
            DefaultAgentNode step = (DefaultAgentNode) agentImplementation;
            Map<String, Object> configuration = step.getConfiguration();
            log.info("Configuration: {}", configuration);
            Map<String, Object> datasourceConfiguration1 = (Map<String, Object>) configuration.get("datasource");
            assertEquals("localhost:1544", datasourceConfiguration1.get("connectionUrl"));
            List<Map<String, Object>> steps = (List<Map<String, Object>>) configuration.get("steps");
            assertEquals(1, steps.size());
        }

        {
            AgentNode agentImplementation = implementation.getAgentImplementation(module, "query2");
            DefaultAgentNode step = (DefaultAgentNode) agentImplementation;
            Map<String, Object> configuration = step.getConfiguration();
            log.info("Configuration: {}", configuration);
            Map<String, Object> datasourceConfiguration1 = (Map<String, Object>) configuration.get("datasource");
            assertEquals("localhost:1545", datasourceConfiguration1.get("connectionUrl"));
            List<Map<String, Object>> steps = (List<Map<String, Object>>) configuration.get("steps");
            // query + cast
            assertEquals(2, steps.size());
        }


        // verify that an intermediate topic is created
        log.info("topics {}", implementation.getTopics().keySet().stream().map(TopicDefinition::getName).toList());
        assertTrue(implementation.getConnectionImplementation(module,
                Connection.from(TopicDefinition.fromName("input-topic"))) instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);
        assertTrue(implementation.getConnectionImplementation(module,
                Connection.from(TopicDefinition.fromName("output-topic"))) instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);
        assertTrue(implementation.getConnectionImplementation(module,
                Connection.from(TopicDefinition.fromName("agent-query2-input"))) instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);




    }

    @Test
    public void testEmbeddingsThanQuery() throws Exception {
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
                                    - name: my-database-1
                                      type: datasource
                                      configuration:
                                        connectionUrl: localhost:1544
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
                                    configuration:                                      
                                      model: "text-embedding-ada-002"
                                      embeddings-field: "value.embeddings"
                                      text: "{{% value.name }} {{% value.description }}"                                  
                                  - name: "query1"
                                    id: query1
                                    type: "query"                                    
                                    configuration:
                                      datasource: "my-database-1"         
                                      query: "select * from table"
                                      output-field: "value.queryresult"                             
                                      fields:
                                        - "value.field1"                                          
                                        - "key.field2"
                                  - name: "casttojson"
                                    type: "cast"                                    
                                    output: "output-topic"
                                    configuration:                                      
                                      schema-type: "string"
                                """));

        @Cleanup ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation("app", applicationInstance);
        log.info("Agents: {}", implementation.getAgents());
        assertEquals(1, implementation.getAgents().size());

        {
            AgentNode agentImplementation = implementation.getAgentImplementation(module, "step1");
            DefaultAgentNode step = (DefaultAgentNode) agentImplementation;
            Map<String, Object> configuration = step.getConfiguration();
            log.info("Configuration: {}", configuration);
            Map<String, Object> datasourceConfiguration1 = (Map<String, Object>) configuration.get("datasource");
            assertEquals("localhost:1544", datasourceConfiguration1.get("connectionUrl"));
            List<Map<String, Object>> steps = (List<Map<String, Object>>) configuration.get("steps");
            assertEquals(3, steps.size());
        }
    }

}