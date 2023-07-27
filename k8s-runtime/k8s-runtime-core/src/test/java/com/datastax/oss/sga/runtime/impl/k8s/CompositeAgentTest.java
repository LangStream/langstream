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
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
    public void testMerge2TextProcessorAgents() throws Exception {
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
            List<Map<String, Object>> agents = (List<Map<String, Object>>) configuration.get("processors");
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
    public void testMerge3TextProcessorAgents() throws Exception {
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
            List<Map<String, Object>> agents = (List<Map<String, Object>>) configuration.get("processors");
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
            List<Map<String, Object>> agents = (List<Map<String, Object>>) configuration.get("processors");
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


    @Test
    public void testMergeSourceWithProcessors() throws Exception {
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
                                  - name: "the-source"
                                    id: "source-1"
                                    type: "s3-source"             
                                    configuration:
                                        paramSource: "source1param"                       
                                  - name: "text-extractor"
                                    id: "step1"
                                    type: "text-extractor"                                                                        
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
            assertEquals(1, implementation.getTopics().size());
            assertTrue(implementation.getConnectionImplementation(module,
                    Connection.from(TopicDefinition.fromName(
                            "output-topic"))) instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);

            assertEquals(1, implementation.getAgents().size());
            AgentNode agentImplementation = implementation.getAgentImplementation(module, "source-1");
            DefaultAgentNode defaultAgentNode = (DefaultAgentNode) agentImplementation;
            Map<String, Object> configuration = defaultAgentNode.getConfiguration();

            Map<String, Object> source = (Map<String, Object>) configuration.get("source");
            assertEquals("s3-source", source.get("agentType"));
            assertEquals("source1param", ((Map<String, Object>) source.get("configuration")).get("paramSource"));

            List<Map<String, Object>> processors = (List<Map<String, Object>>) configuration.get("processors");
            assertEquals(3, processors.size());
            assertEquals("text-extractor", processors.get(0).get("agentType"));
            assertEquals("value1", ((Map<String, Object>) processors.get(0).get("configuration")).get("param1"));
            assertEquals("language-detector", processors.get(1).get("agentType"));
            assertEquals("value2", ((Map<String, Object>) processors.get(1).get("configuration")).get("param2"));
            assertEquals("language-detector", processors.get(2).get("agentType"));
            assertEquals("value3", ((Map<String, Object>) processors.get(2).get("configuration")).get("param3"));

            Topic outputTopic = (Topic) defaultAgentNode.getOutputConnection();
            assertEquals("output-topic", outputTopic.topicName());

            assertNull(defaultAgentNode.getInputConnection());
        }
    }

    @Test
    public void testMergeSinkWithProcessorAgents() throws Exception {
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
                                    configuration:                                      
                                      param3: "value3"
                                  - name: "generic-composable-sink"                                    
                                    type: "generic-composable-sink"
                                    configuration:                                      
                                      paramSink: "sink1param"
                                """));

        try (ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();) {


            ExecutionPlan implementation = deployer.createImplementation("app", applicationInstance);


            Module module = applicationInstance.getModule("module-1");
            assertEquals(1, implementation.getAgents().size());
            assertEquals(1, implementation.getTopics().size());
            assertTrue(implementation.getConnectionImplementation(module,
                    Connection.from(TopicDefinition.fromName(
                            "input-topic"))) instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);

            assertEquals(1, implementation.getAgents().size());
            AgentNode agentImplementation = implementation.getAgentImplementation(module, "step1");
            DefaultAgentNode defaultAgentNode = (DefaultAgentNode) agentImplementation;
            Map<String, Object> configuration = defaultAgentNode.getConfiguration();
            List<Map<String, Object>> processors = (List<Map<String, Object>>) configuration.get("processors");
            assertEquals(3, processors.size());
            assertEquals("text-extractor", processors.get(0).get("agentType"));
            assertEquals("value1", ((Map<String, Object>) processors.get(0).get("configuration")).get("param1"));
            assertEquals("language-detector", processors.get(1).get("agentType"));
            assertEquals("value2", ((Map<String, Object>) processors.get(1).get("configuration")).get("param2"));
            assertEquals("language-detector", processors.get(2).get("agentType"));
            assertEquals("value3", ((Map<String, Object>) processors.get(2).get("configuration")).get("param3"));

            Topic inputTopic = (Topic) defaultAgentNode.getInputConnection();
            assertEquals("input-topic", inputTopic.topicName());
            assertNull(defaultAgentNode.getOutputConnection());


            Map<String, Object> sink = (Map<String, Object>) configuration.get("sink");
            assertEquals("generic-composable-sink", sink.get("agentType"));
            assertEquals("sink1param", ((Map<String, Object>) sink.get("configuration")).get("paramSink"));
        }
    }


    @Test
    public void testMergeSourceAndSinkWithProcessorAgents() throws Exception {
        Application applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of("instance.yaml",
                        buildInstanceYaml(),
                        "module.yaml", """
                                module: "module-1"
                                id: "pipeline-1"                                
                                pipeline:
                                  - name: "the-source"
                                    id: "source-1"
                                    type: "s3-source"             
                                    configuration:
                                        paramSource: "source1param"      
                                  - name: "text-extractor"
                                    id: "step1"
                                    type: "text-extractor"                                    
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
                                    configuration:                                      
                                      param3: "value3"
                                  - name: "generic-composable-sink"                                    
                                    type: "generic-composable-sink"
                                    configuration:                                      
                                      paramSink: "sink1param"
                                """));

        try (ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();) {


            ExecutionPlan implementation = deployer.createImplementation("app", applicationInstance);


            Module module = applicationInstance.getModule("module-1");
            assertEquals(1, implementation.getAgents().size());
            assertEquals(0, implementation.getTopics().size());

            assertEquals(1, implementation.getAgents().size());
            AgentNode agentImplementation = implementation.getAgentImplementation(module, "source-1");
            DefaultAgentNode defaultAgentNode = (DefaultAgentNode) agentImplementation;
            Map<String, Object> configuration = defaultAgentNode.getConfiguration();

            Map<String, Object> source = (Map<String, Object>) configuration.get("source");
            assertEquals("s3-source", source.get("agentType"));
            assertEquals("source1param", ((Map<String, Object>) source.get("configuration")).get("paramSource"));

            List<Map<String, Object>> processors = (List<Map<String, Object>>) configuration.get("processors");
            assertEquals(3, processors.size());
            assertEquals("text-extractor", processors.get(0).get("agentType"));
            assertEquals("value1", ((Map<String, Object>) processors.get(0).get("configuration")).get("param1"));
            assertEquals("language-detector", processors.get(1).get("agentType"));
            assertEquals("value2", ((Map<String, Object>) processors.get(1).get("configuration")).get("param2"));
            assertEquals("language-detector", processors.get(2).get("agentType"));
            assertEquals("value3", ((Map<String, Object>) processors.get(2).get("configuration")).get("param3"));

            assertNull(defaultAgentNode.getInputConnection());
            assertNull(defaultAgentNode.getOutputConnection());


            Map<String, Object> sink = (Map<String, Object>) configuration.get("sink");
            assertEquals("generic-composable-sink", sink.get("agentType"));
            assertEquals("sink1param", ((Map<String, Object>) sink.get("configuration")).get("paramSink"));
        }
    }


    @Test
    public void testMergeSourceAndSinkWithProcessorAgentsWithIntermediateTopics() throws Exception {
        Application applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of("instance.yaml",
                        buildInstanceYaml(),
                        "module.yaml", """
                                module: "module-1"
                                id: "pipeline-1"                                
                                pipeline:
                                  - name: "the-source"
                                    id: "source-1"
                                    type: "s3-source"             
                                    configuration:
                                        paramSource: "source1param"      
                                  - name: "text-extractor"
                                    id: "step1"
                                    type: "text-extractor"                                    
                                    configuration:                                      
                                      param1: "value1"
                                  - name: "language-detector"
                                    id: "step2"
                                    type: "language-detector"                                    
                                    configuration:                                      
                                      param2: "value2"
                                  - name: "requires-buffer-topic"
                                    id: "bad-step"
                                    type: "drop"
                                  - name: "language-detector-2"
                                    id: "step3"
                                    type: "language-detector"
                                    configuration:                                      
                                      param3: "value3"
                                  - name: "generic-composable-sink"                                    
                                    type: "generic-composable-sink"
                                    configuration:                                      
                                      paramSink: "sink1param"
                                """));

        try (ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();) {


            ExecutionPlan implementation = deployer.createImplementation("app", applicationInstance);


            Module module = applicationInstance.getModule("module-1");

            log.info("Agents {}", implementation.getAgents().keySet());
            log.info("Topics {}", implementation.getTopics().values().stream().map(Topic::topicName).collect(Collectors.toList()));

            assertEquals(3, implementation.getAgents().size());
            assertEquals(2, implementation.getTopics().size());

            assertNotNull(implementation.getTopicByName("agent-step3-input"));
            assertNotNull(implementation.getTopicByName("agent-bad-step-input"));

            AgentNode firstNode = implementation.getAgentImplementation(module, "source-1");
            DefaultAgentNode defaultFirstNode = (DefaultAgentNode) firstNode;
            Map<String, Object> configuration = defaultFirstNode.getConfiguration();

            Map<String, Object> source = (Map<String, Object>) configuration.get("source");
            assertEquals("s3-source", source.get("agentType"));
            assertEquals("source1param", ((Map<String, Object>) source.get("configuration")).get("paramSource"));

            List<Map<String, Object>> processors = (List<Map<String, Object>>) configuration.get("processors");
            assertEquals(2, processors.size());
            assertEquals("text-extractor", processors.get(0).get("agentType"));
            assertEquals("value1", ((Map<String, Object>) processors.get(0).get("configuration")).get("param1"));
            assertEquals("language-detector", processors.get(1).get("agentType"));
            assertEquals("value2", ((Map<String, Object>) processors.get(1).get("configuration")).get("param2"));

            Topic outputTopic = (Topic) defaultFirstNode.getOutputConnection();
            assertEquals("agent-bad-step-input", outputTopic.topicName());
            assertNull(defaultFirstNode.getInputConnection());


            AgentNode secondNode = implementation.getAgentImplementation(module, "bad-step");
            DefaultAgentNode defaultSecondNode = (DefaultAgentNode) secondNode;
            Map<String, Object> configurationSecondNode = defaultSecondNode.getConfiguration();
            assertEquals("ai-tools", defaultSecondNode.getAgentType());

            Topic inputTopicSecondStep = (Topic) defaultSecondNode.getInputConnection();
            assertEquals("agent-bad-step-input", inputTopicSecondStep.topicName());
            Topic outputTopicSecondStep = (Topic) defaultSecondNode.getOutputConnection();
            assertEquals("agent-step3-input", outputTopicSecondStep.topicName());

            AgentNode thirdNode = implementation.getAgentImplementation(module, "step3");
            DefaultAgentNode defaultThirdNode = (DefaultAgentNode) thirdNode;
            Map<String, Object> configurationThirdNode = defaultThirdNode.getConfiguration();
            assertEquals("composite-agent", defaultThirdNode.getAgentType());


            processors = (List<Map<String, Object>>) configurationThirdNode.get("processors");
            assertEquals(1, processors.size());
            assertEquals("language-detector", processors.get(0).get("agentType"));
            assertEquals("value3", ((Map<String, Object>) processors.get(0).get("configuration")).get("param3"));


            Map<String, Object> sink = (Map<String, Object>) configurationThirdNode.get("sink");
            assertEquals("generic-composable-sink", sink.get("agentType"));
            assertEquals("sink1param", ((Map<String, Object>) sink.get("configuration")).get("paramSink"));


            Topic inputTopic = (Topic) defaultThirdNode.getInputConnection();
            assertEquals("agent-step3-input", inputTopic.topicName());
            assertNull(defaultThirdNode.getOutputConnection());

        }
    }

}