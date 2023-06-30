package com.datastax.oss.sga.pulsar;

import com.datastax.oss.sga.api.model.ApplicationInstance;
import com.datastax.oss.sga.api.model.Connection;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.TopicDefinition;
import com.datastax.oss.sga.api.runtime.AgentImplementation;
import com.datastax.oss.sga.api.runtime.ClusterRuntimeRegistry;
import com.datastax.oss.sga.api.runtime.PluginsRegistry;
import com.datastax.oss.sga.impl.common.AbstractAgentProvider;
import com.datastax.oss.sga.impl.deploy.ApplicationDeployer;
import com.datastax.oss.sga.impl.parser.ModelBuilder;
import com.datastax.oss.sga.pulsar.agents.AbstractPulsarFunctionAgentProvider;
import com.datastax.oss.sga.pulsar.agents.AbstractPulsarSinkAgentProvider;
import com.datastax.oss.sga.pulsar.agents.AbstractPulsarSourceAgentProvider;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
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
        ApplicationInstance applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of("instance.yaml",
                        """
                                instance:
                                  streamingCluster:
                                    type: "pulsar"
                                    configuration:                                      
                                      webServiceUrl: "http://localhost:8080"
                                      defaultTenant: "public"
                                      defaultNamespace: "default"
                                """,
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

        ApplicationDeployer<PulsarPhysicalApplicationInstance> deployer = ApplicationDeployer
                .<PulsarPhysicalApplicationInstance>builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        Module module = applicationInstance.getModule("module-1");

        PulsarPhysicalApplicationInstance implementation = deployer.createImplementation(applicationInstance);
        assertTrue(implementation.getConnectionImplementation(module, new Connection(new TopicDefinition("input-topic-cassandra", null, null))) instanceof PulsarTopic);
        PulsarName pulsarName = new PulsarName("public", "default", "input-topic-cassandra");
        assertTrue(implementation.getTopics().containsKey(pulsarName));

        AgentImplementation agentImplementation = implementation.getAgentImplementation(module, "sink-1-id");
        assertNotNull(agentImplementation);
        AbstractAgentProvider.DefaultAgentImplementation genericSink = (AbstractAgentProvider.DefaultAgentImplementation) agentImplementation;
        AbstractPulsarSinkAgentProvider.PulsarSinkMetadata pulsarSinkMetadata = genericSink.getPhysicalMetadata();
        assertEquals("cassandra-enhanced", pulsarSinkMetadata.getSinkType());
        assertEquals(new PulsarName("public", "default", "sink1"), pulsarSinkMetadata.getPulsarName());

    }

    @Test
    public void testMapGenericPulsarSink() throws Exception {
        ApplicationInstance applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of("instance.yaml",
                        """
                                instance:
                                  streamingCluster:
                                    type: "pulsar"
                                    configuration:                                      
                                      webServiceUrl: "http://localhost:8080"
                                      defaultTenant: "public"
                                      defaultNamespace: "default"
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

        ApplicationDeployer<PulsarPhysicalApplicationInstance> deployer = ApplicationDeployer
                .<PulsarPhysicalApplicationInstance>builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        Module module = applicationInstance.getModule("module-1");

        PulsarPhysicalApplicationInstance implementation = deployer.createImplementation(applicationInstance);
        assertTrue(implementation.getConnectionImplementation(module, new Connection(new TopicDefinition("input-topic", null, null))) instanceof PulsarTopic);
        PulsarName pulsarName = new PulsarName("public", "default", "input-topic");
        assertTrue(implementation.getTopics().containsKey(pulsarName));

        AgentImplementation agentImplementation = implementation.getAgentImplementation(module, "sink-1-id");
        assertNotNull(agentImplementation);
        AbstractAgentProvider.DefaultAgentImplementation genericSink = (AbstractAgentProvider.DefaultAgentImplementation) agentImplementation;
        AbstractPulsarSinkAgentProvider.PulsarSinkMetadata pulsarSinkMetadata = genericSink.getPhysicalMetadata();
        assertEquals("some-sink-type-on-your-cluster", pulsarSinkMetadata.getSinkType());
        assertEquals(new PulsarName("public", "default", "sink1"), pulsarSinkMetadata.getPulsarName());

    }

    @Test
    public void testMapGenericPulsarSource() throws Exception {
        ApplicationInstance applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of("instance.yaml",
                        """
                                instance:
                                  streamingCluster:
                                    type: "pulsar"
                                    configuration:                                      
                                      webServiceUrl: "http://localhost:8080"
                                      defaultTenant: "public"
                                      defaultNamespace: "default"
                                """,
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

        ApplicationDeployer<PulsarPhysicalApplicationInstance> deployer = ApplicationDeployer
                .<PulsarPhysicalApplicationInstance>builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        Module module = applicationInstance.getModule("module-1");

        PulsarPhysicalApplicationInstance implementation = deployer.createImplementation(applicationInstance);
        assertTrue(implementation.getConnectionImplementation(module, new Connection(new TopicDefinition("output-topic", null, null))) instanceof PulsarTopic);
        PulsarName pulsarName = new PulsarName("public", "default", "output-topic");
        assertTrue(implementation.getTopics().containsKey(pulsarName));

        AgentImplementation agentImplementation = implementation.getAgentImplementation(module, "source-1-id");
        assertNotNull(agentImplementation);
        AbstractAgentProvider.DefaultAgentImplementation genericSink = (AbstractAgentProvider.DefaultAgentImplementation) agentImplementation;
        AbstractPulsarSourceAgentProvider.PulsarSourceMetadata pulsarSourceMetadata = genericSink.getPhysicalMetadata();
        assertEquals("some-source-type-on-your-cluster", pulsarSourceMetadata.getSourceType());
        assertEquals(new PulsarName("public", "default", "source1"), pulsarSourceMetadata.getPulsarName());

    }


    @Test
    public void testMapGenericPulsarFunction() throws Exception {
        ApplicationInstance applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of("instance.yaml",
                        """
                                instance:
                                  streamingCluster:
                                    type: "pulsar"
                                    configuration:                                      
                                      webServiceUrl: "http://localhost:8080"
                                      defaultTenant: "public"
                                      defaultNamespace: "default"
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

        ApplicationDeployer<PulsarPhysicalApplicationInstance> deployer = ApplicationDeployer
                .<PulsarPhysicalApplicationInstance>builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        Module module = applicationInstance.getModule("module-1");

        PulsarPhysicalApplicationInstance implementation = deployer.createImplementation(applicationInstance);
        {
            assertTrue(implementation.getConnectionImplementation(module, new Connection(new TopicDefinition("input-topic", null, null))) instanceof PulsarTopic);
            PulsarName pulsarName = new PulsarName("public", "default", "input-topic");
            assertTrue(implementation.getTopics().containsKey(pulsarName));
        }
        {
            assertTrue(implementation.getConnectionImplementation(module, new Connection(new TopicDefinition("output-topic", null, null))) instanceof PulsarTopic);
            PulsarName pulsarName = new PulsarName("public", "default", "output-topic");
            assertTrue(implementation.getTopics().containsKey(pulsarName));
        }

        AgentImplementation agentImplementation = implementation.getAgentImplementation(module, "function-1-id");
        assertNotNull(agentImplementation);
        AbstractAgentProvider.DefaultAgentImplementation genericSink = (AbstractAgentProvider.DefaultAgentImplementation) agentImplementation;
        AbstractPulsarFunctionAgentProvider.PulsarFunctionMetadata pulsarSourceMetadata = genericSink.getPhysicalMetadata();
        assertEquals("some-function-type-on-your-cluster", pulsarSourceMetadata.getFunctionType());
        assertEquals("a.b.c.ClassName", pulsarSourceMetadata.getFunctionClassname());
        assertEquals(new PulsarName("public", "default", "function1"), pulsarSourceMetadata.getPulsarName());

    }




    @Test
    public void testMapGenericPulsarFunctionsChain() throws Exception {
        ApplicationInstance applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of("instance.yaml",
                        """
                                instance:
                                  streamingCluster:
                                    type: "pulsar"
                                    configuration:                                      
                                      webServiceUrl: "http://localhost:8080"
                                      defaultTenant: "public"
                                      defaultNamespace: "default"
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

        ApplicationDeployer<PulsarPhysicalApplicationInstance> deployer = ApplicationDeployer
                .<PulsarPhysicalApplicationInstance>builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        Module module = applicationInstance.getModule("module-1");

        PulsarPhysicalApplicationInstance implementation = deployer.createImplementation(applicationInstance);
        {
            assertTrue(implementation.getConnectionImplementation(module, new Connection(new TopicDefinition("input-topic", null, null))) instanceof PulsarTopic);
            PulsarName pulsarName = new PulsarName("public", "default", "input-topic");
            assertTrue(implementation.getTopics().containsKey(pulsarName));
        }
        {
            assertTrue(implementation.getConnectionImplementation(module, new Connection(new TopicDefinition("output-topic", null, null))) instanceof PulsarTopic);
            PulsarName pulsarName = new PulsarName("public", "default", "output-topic");
            assertTrue(implementation.getTopics().containsKey(pulsarName));
        }

        {
            assertTrue(implementation.getConnectionImplementation(module, new Connection(new TopicDefinition("agent-function-1-id-output", null, null))) instanceof PulsarTopic);
            PulsarName pulsarName = new PulsarName("public", "default", "agent-function-1-id-output");
            assertTrue(implementation.getTopics().containsKey(pulsarName));
        }



        assertEquals(3, implementation.getTopics().size());

        {
            AgentImplementation agentImplementation = implementation.getAgentImplementation(module, "function-1-id");
            assertNotNull(agentImplementation);
            AbstractAgentProvider.DefaultAgentImplementation genericSink = (AbstractAgentProvider.DefaultAgentImplementation) agentImplementation;
            AbstractPulsarFunctionAgentProvider.PulsarFunctionMetadata pulsarSourceMetadata = genericSink.getPhysicalMetadata();
            assertEquals("some-function-type-on-your-cluster", pulsarSourceMetadata.getFunctionType());
            assertEquals("a.b.c.ClassName", pulsarSourceMetadata.getFunctionClassname());
            assertEquals(new PulsarName("public", "default", "function1"), pulsarSourceMetadata.getPulsarName());
        }

        {
            AgentImplementation agentImplementation = implementation.getAgentImplementation(module, "function-2-id");
            assertNotNull(agentImplementation);
            AbstractAgentProvider.DefaultAgentImplementation genericSink = (AbstractAgentProvider.DefaultAgentImplementation) agentImplementation;
            AbstractPulsarFunctionAgentProvider.PulsarFunctionMetadata pulsarSourceMetadata = genericSink.getPhysicalMetadata();
            assertEquals("some-function-type-on-your-cluster", pulsarSourceMetadata.getFunctionType());
            assertEquals("a.b.c.ClassName", pulsarSourceMetadata.getFunctionClassname());
            assertEquals(new PulsarName("public", "default", "function2"), pulsarSourceMetadata.getPulsarName());
        }

    }


    @Test
    public void testOpenAIComputeEmbeddingFunction() throws Exception {
        ApplicationInstance applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of("instance.yaml",
                        """
                                instance:
                                  streamingCluster:
                                    type: "pulsar"
                                    configuration:                                      
                                      webServiceUrl: "http://localhost:8080"
                                      defaultTenant: "public"
                                      defaultNamespace: "default"
                                """,
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
                                      text: "{{ value.name }} {{ value.description }}"
                                """));

        ApplicationDeployer<PulsarPhysicalApplicationInstance> deployer = ApplicationDeployer
                .<PulsarPhysicalApplicationInstance>builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        Module module = applicationInstance.getModule("module-1");

        PulsarPhysicalApplicationInstance implementation = deployer.createImplementation(applicationInstance);
        assertTrue(implementation.getConnectionImplementation(module, new Connection(new TopicDefinition("input-topic", null, null))) instanceof PulsarTopic);
        assertTrue(implementation.getConnectionImplementation(module, new Connection(new TopicDefinition("output-topic", null, null))) instanceof PulsarTopic);

        AgentImplementation agentImplementation = implementation.getAgentImplementation(module, "step1");
        assertNotNull(agentImplementation);
        AbstractAgentProvider.DefaultAgentImplementation step = (AbstractAgentProvider.DefaultAgentImplementation) agentImplementation;
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