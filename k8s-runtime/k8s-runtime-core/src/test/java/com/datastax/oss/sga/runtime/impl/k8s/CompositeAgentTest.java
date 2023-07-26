package com.datastax.oss.sga.runtime.impl.k8s;

import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.Connection;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.TopicDefinition;
import com.datastax.oss.sga.api.runtime.AgentNode;
import com.datastax.oss.sga.api.runtime.ClusterRuntimeRegistry;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.PluginsRegistry;
import com.datastax.oss.sga.api.runtime.Topic;
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
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class CompositeAgentTest {

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
    public void testMerge2TikaAgents() throws Exception {
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
                                  - name: "text-extractor"
                                    id: "step1"
                                    type: "text-extractor"
                                    input: "input-topic"                                    
                                    configuration:                                      
                                      param1: "value1"
                                  - name: "language-detector"
                                    id: "step2"
                                    type: "language-detector"
                                    output: "output-topic"                                    
                                    configuration:                                      
                                      param2: "value2"
                                """));

        try (ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();) {


            ExecutionPlan implementation = deployer.createImplementation("app", applicationInstance);


            Module module = applicationInstance.getModule("module-1");
            assertEquals(1, implementation.getAgents().size());
            assertEquals(2, implementation.getTopics().size());
            assertTrue(implementation.getConnectionImplementation(module,
                    Connection.from(TopicDefinition.fromName(
                            "input-topic"))) instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);
            assertTrue(implementation.getConnectionImplementation(module,
                    Connection.from(TopicDefinition.fromName(
                            "output-topic"))) instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);

            assertEquals(1, implementation.getAgents().size());
            AgentNode agentImplementation = implementation.getAgentImplementation(module, "step1");
            DefaultAgentNode defaultAgentNode = (DefaultAgentNode) agentImplementation;
            Map<String, Object> configuration = defaultAgentNode.getConfiguration();
            List<Map<String, Object>> agents = (List<Map<String, Object>>) configuration.get("agents");
            assertEquals(2, agents.size());
            assertEquals("text-extractor", agents.get(0).get("agentType"));
            assertEquals("value1", ((Map<String, Object>) agents.get(0).get("configuration")).get("param1"));
            assertEquals("language-detector", agents.get(1).get("agentType"));
            assertEquals("value2", ((Map<String, Object>) agents.get(1).get("configuration")).get("param2"));

            Topic inputTopic = (Topic) defaultAgentNode.getInputConnection();
            assertEquals("input-topic", inputTopic.topicName());
            Topic outputTopic = (Topic) defaultAgentNode.getOutputConnection();
            assertEquals("output-topic", outputTopic.topicName());


        }


    }

    @Test
    public void testMerge3TikaAgents() throws Exception {
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
                                  - name: "text-extractor"
                                    id: "step1"
                                    type: "text-extractor"
                                    input: "input-topic"                                    
                                    configuration:                                      
                                      param1: "value1"
                                  - name: "language-detector"
                                    id: "step2"
                                    type: "language-detector"                                    
                                    configuration:                                      
                                      param2: "value2"
                                  - name: "language-detector-2"
                                    id: "step3"
                                    type: "language-detector"
                                    output: "output-topic"                                    
                                    configuration:                                      
                                      param3: "value3"
                                """));

        try (ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();) {


            ExecutionPlan implementation = deployer.createImplementation("app", applicationInstance);


            Module module = applicationInstance.getModule("module-1");
            assertEquals(1, implementation.getAgents().size());
            assertEquals(2, implementation.getTopics().size());
            assertTrue(implementation.getConnectionImplementation(module,
                    Connection.from(TopicDefinition.fromName(
                            "input-topic"))) instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);
            assertTrue(implementation.getConnectionImplementation(module,
                    Connection.from(TopicDefinition.fromName(
                            "output-topic"))) instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);

            assertEquals(1, implementation.getAgents().size());
            AgentNode agentImplementation = implementation.getAgentImplementation(module, "step1");
            DefaultAgentNode defaultAgentNode = (DefaultAgentNode) agentImplementation;
            Map<String, Object> configuration = defaultAgentNode.getConfiguration();
            List<Map<String, Object>> agents = (List<Map<String, Object>>) configuration.get("agents");
            assertEquals(3, agents.size());
            assertEquals("text-extractor", agents.get(0).get("agentType"));
            assertEquals("value1", ((Map<String, Object>) agents.get(0).get("configuration")).get("param1"));
            assertEquals("language-detector", agents.get(1).get("agentType"));
            assertEquals("value2", ((Map<String, Object>) agents.get(1).get("configuration")).get("param2"));
            assertEquals("language-detector", agents.get(2).get("agentType"));
            assertEquals("value3", ((Map<String, Object>) agents.get(2).get("configuration")).get("param3"));

            Topic inputTopic = (Topic) defaultAgentNode.getInputConnection();
            assertEquals("input-topic", inputTopic.topicName());
            Topic outputTopic = (Topic) defaultAgentNode.getOutputConnection();
            assertEquals("output-topic", outputTopic.topicName());
        }


    }

    @Test
    public void testMixWithNonComposableAgents() throws Exception {
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
                                  - name: "text-extractor"
                                    id: "step1"
                                    type: "text-extractor"
                                    input: "input-topic"                                    
                                    configuration:                                      
                                      param1: "value1"
                                  - name: "language-detector"
                                    id: "step1b"
                                    type: "language-detector"                                    
                                    configuration:                                      
                                      param2: "value2"
                                  - name: "drop-if-not-english"
                                    id: "step2"
                                    type: "drop"                                    
                                    configuration:                                      
                                      when: "properties.language != 'en'"
                                  - name: "language-detector-2"
                                    id: "step3"
                                    type: "language-detector"
                                    output: "output-topic"                                    
                                    configuration:                                      
                                      param3: "value3"
                                """));

        try (ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();) {


            ExecutionPlan implementation = deployer.createImplementation("app", applicationInstance);


            Module module = applicationInstance.getModule("module-1");
            assertEquals(3, implementation.getAgents().size());
            log.info("Topics {}", implementation.getTopics().values().stream().map(Topic::topicName).collect(Collectors.toList()));
            assertEquals(4, implementation.getTopics().size());
            assertTrue(implementation.getConnectionImplementation(module,
                    Connection.from(TopicDefinition.fromName(
                            "input-topic"))) instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);
            assertTrue(implementation.getConnectionImplementation(module,
                    Connection.from(TopicDefinition.fromName(
                            "output-topic"))) instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);
            assertTrue(implementation.getConnectionImplementation(module,
                    Connection.from(TopicDefinition.fromName(
                            "agent-step2-input"))) instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);
            assertTrue(implementation.getConnectionImplementation(module,
                    Connection.from(TopicDefinition.fromName(
                            "agent-step3-input"))) instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);

            assertEquals(3, implementation.getAgents().size());

            AgentNode agentImplementation = implementation.getAgentImplementation(module, "step1");
            DefaultAgentNode defaultAgentNode = (DefaultAgentNode) agentImplementation;
            Map<String, Object> configuration = defaultAgentNode.getConfiguration();
            List<Map<String, Object>> agents = (List<Map<String, Object>>) configuration.get("agents");
            assertEquals(2, agents.size());
            assertEquals("text-extractor", agents.get(0).get("agentType"));
            assertEquals("value1", ((Map<String, Object>) agents.get(0).get("configuration")).get("param1"));
            assertEquals("language-detector", agents.get(1).get("agentType"));
            assertEquals("value2", ((Map<String, Object>) agents.get(1).get("configuration")).get("param2"));

            Topic inputTopic = (Topic) defaultAgentNode.getInputConnection();
            assertEquals("input-topic", inputTopic.topicName());
            Topic outputTopic = (Topic) defaultAgentNode.getOutputConnection();
            assertEquals("agent-step2-input", outputTopic.topicName());

            AgentNode agentImplementationDrop = implementation.getAgentImplementation(module, "step2");
            DefaultAgentNode defaultAgentNodeDrop = (DefaultAgentNode) agentImplementationDrop;
            Map<String, Object> configurationDrop = defaultAgentNodeDrop.getConfiguration();
            assertNotNull(configurationDrop.get("steps"));
            assertEquals("ai-tools", defaultAgentNodeDrop.getAgentType());

            Topic inputTopicDrop = (Topic) defaultAgentNodeDrop.getInputConnection();
            assertEquals("agent-step2-input", inputTopicDrop.topicName());
            Topic outputTopicDrop = (Topic) defaultAgentNodeDrop.getOutputConnection();
            assertEquals("agent-step3-input", outputTopicDrop.topicName());

            AgentNode agentImplementationLast = implementation.getAgentImplementation(module, "step3");
            DefaultAgentNode defaultAgentNodeLast = (DefaultAgentNode) agentImplementationLast;
            Map<String, Object> configurationLast = defaultAgentNodeLast.getConfiguration();
            assertEquals("value3", configurationLast.get("param3"));

            Topic inputTopicLast = (Topic) defaultAgentNodeLast.getInputConnection();
            assertEquals("agent-step3-input", inputTopicLast.topicName());
            Topic outputTopicLast = (Topic) defaultAgentNodeLast.getOutputConnection();
            assertEquals("output-topic", outputTopicLast.topicName());


        }


    }

}