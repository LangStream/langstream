/*
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
package ai.langstream.runtime.impl.k8s;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.langstream.api.model.Application;
import ai.langstream.api.model.Connection;
import ai.langstream.api.model.Module;
import ai.langstream.api.model.TopicDefinition;
import ai.langstream.api.runtime.AgentNode;
import ai.langstream.api.runtime.ClusterRuntimeRegistry;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.api.runtime.Topic;
import ai.langstream.impl.agents.AbstractCompositeAgentProvider;
import ai.langstream.impl.common.DefaultAgentNode;
import ai.langstream.impl.deploy.ApplicationDeployer;
import ai.langstream.impl.noop.NoOpStreamingClusterRuntimeProvider;
import ai.langstream.impl.parser.ModelBuilder;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

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
        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
                                        "module.yaml",
                                        """
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
                                  - name: "language-detector"
                                    id: "step2"
                                    type: "language-detector"
                                    output: "output-topic"
                                    configuration:
                                      property: "value2"
                                """),
                                buildInstanceYaml(),
                                null)
                        .getApplication();

        try (ApplicationDeployer deployer =
                ApplicationDeployer.builder()
                        .registry(new ClusterRuntimeRegistry())
                        .pluginsRegistry(new PluginsRegistry())
                        .build()) {

            ExecutionPlan implementation =
                    deployer.createImplementation("app", applicationInstance);

            Module module = applicationInstance.getModule("module-1");
            assertEquals(1, implementation.getAgents().size());
            assertEquals(2, implementation.getTopics().size());
            assertTrue(
                    implementation.getConnectionImplementation(
                                    module,
                                    Connection.fromTopic(TopicDefinition.fromName("input-topic")))
                            instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);
            assertTrue(
                    implementation.getConnectionImplementation(
                                    module,
                                    Connection.fromTopic(TopicDefinition.fromName("output-topic")))
                            instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);

            assertEquals(1, implementation.getAgents().size());
            AgentNode agentImplementation = implementation.getAgentImplementation(module, "step1");
            DefaultAgentNode defaultAgentNode = (DefaultAgentNode) agentImplementation;
            Map<String, Object> configuration = defaultAgentNode.getConfiguration();
            List<Map<String, Object>> agents =
                    (List<Map<String, Object>>) configuration.get("processors");
            assertEquals(2, agents.size());
            assertEquals("text-extractor", agents.get(0).get("agentType"));
            assertEquals("language-detector", agents.get(1).get("agentType"));
            assertEquals(
                    "value2",
                    ((Map<String, Object>) agents.get(1).get("configuration")).get("property"));

            Topic inputTopic = (Topic) defaultAgentNode.getInputConnectionImplementation();
            assertEquals("input-topic", inputTopic.topicName());
            Topic outputTopic = (Topic) defaultAgentNode.getOutputConnectionImplementation();
            assertEquals("output-topic", outputTopic.topicName());
        }
    }

    @Test
    public void testMerge3TextProcessorAgents() throws Exception {
        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
                                        "module.yaml",
                                        """
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
                                  - name: "language-detector"
                                    id: "step2"
                                    type: "language-detector"
                                    configuration:
                                      property: "value2"
                                  - name: "language-detector-2"
                                    id: "step3"
                                    type: "language-detector"
                                    output: "output-topic"
                                    configuration:
                                      allowedLanguages: ["en"]
                                """),
                                buildInstanceYaml(),
                                null)
                        .getApplication();

        try (ApplicationDeployer deployer =
                ApplicationDeployer.builder()
                        .registry(new ClusterRuntimeRegistry())
                        .pluginsRegistry(new PluginsRegistry())
                        .build()) {

            ExecutionPlan implementation =
                    deployer.createImplementation("app", applicationInstance);

            Module module = applicationInstance.getModule("module-1");
            assertEquals(1, implementation.getAgents().size());
            assertEquals(2, implementation.getTopics().size());
            assertTrue(
                    implementation.getConnectionImplementation(
                                    module,
                                    Connection.fromTopic(TopicDefinition.fromName("input-topic")))
                            instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);
            assertTrue(
                    implementation.getConnectionImplementation(
                                    module,
                                    Connection.fromTopic(TopicDefinition.fromName("output-topic")))
                            instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);

            assertEquals(1, implementation.getAgents().size());
            AgentNode agentImplementation = implementation.getAgentImplementation(module, "step1");
            DefaultAgentNode defaultAgentNode = (DefaultAgentNode) agentImplementation;
            Map<String, Object> configuration = defaultAgentNode.getConfiguration();
            List<Map<String, Object>> agents =
                    (List<Map<String, Object>>) configuration.get("processors");
            assertEquals(3, agents.size());
            assertEquals("text-extractor", agents.get(0).get("agentType"));
            assertEquals("language-detector", agents.get(1).get("agentType"));
            assertEquals(
                    "value2",
                    ((Map<String, Object>) agents.get(1).get("configuration")).get("property"));
            assertEquals("language-detector", agents.get(2).get("agentType"));
            assertEquals(
                    List.of("en"),
                    ((Map<String, Object>) agents.get(2).get("configuration"))
                            .get("allowedLanguages"));

            Topic inputTopic = (Topic) defaultAgentNode.getInputConnectionImplementation();
            assertEquals("input-topic", inputTopic.topicName());
            Topic outputTopic = (Topic) defaultAgentNode.getOutputConnectionImplementation();
            assertEquals("output-topic", outputTopic.topicName());
        }
    }

    @Test
    public void testMixWithNonComposableAgents() throws Exception {
        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
                                        "module.yaml",
                                        """
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
                                  - name: "language-detector"
                                    id: "step1b"
                                    type: "language-detector"
                                    configuration:
                                      property: "value2"
                                  - name: "drop-if-not-english"
                                    id: "step2"
                                    type: "drop"
                                    configuration:
                                      when: "properties.language != 'en'"
                                      composable: false
                                  - name: "language-detector-2"
                                    id: "step3"
                                    type: "language-detector"
                                    output: "output-topic"
                                    configuration:
                                      allowedLanguages: ["en"]
                                """),
                                buildInstanceYaml(),
                                null)
                        .getApplication();

        try (ApplicationDeployer deployer =
                ApplicationDeployer.builder()
                        .registry(new ClusterRuntimeRegistry())
                        .pluginsRegistry(new PluginsRegistry())
                        .build()) {

            ExecutionPlan implementation =
                    deployer.createImplementation("app", applicationInstance);

            Module module = applicationInstance.getModule("module-1");
            assertEquals(3, implementation.getAgents().size());
            log.info(
                    "Topics {}",
                    implementation.getTopics().values().stream()
                            .map(Topic::topicName)
                            .collect(Collectors.toList()));
            assertEquals(4, implementation.getTopics().size());
            assertTrue(
                    implementation.getConnectionImplementation(
                                    module,
                                    Connection.fromTopic(TopicDefinition.fromName("input-topic")))
                            instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);
            assertTrue(
                    implementation.getConnectionImplementation(
                                    module,
                                    Connection.fromTopic(TopicDefinition.fromName("output-topic")))
                            instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);
            assertTrue(
                    implementation.getConnectionImplementation(
                                    module,
                                    Connection.fromTopic(
                                            TopicDefinition.fromName("agent-step2-input")))
                            instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);
            assertTrue(
                    implementation.getConnectionImplementation(
                                    module,
                                    Connection.fromTopic(
                                            TopicDefinition.fromName("agent-step3-input")))
                            instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);

            assertEquals(3, implementation.getAgents().size());

            AgentNode agentImplementation = implementation.getAgentImplementation(module, "step1");
            DefaultAgentNode defaultAgentNode = (DefaultAgentNode) agentImplementation;
            Map<String, Object> configuration = defaultAgentNode.getConfiguration();
            List<Map<String, Object>> agents =
                    (List<Map<String, Object>>) configuration.get("processors");
            assertEquals(2, agents.size());
            assertEquals("text-extractor", agents.get(0).get("agentType"));
            assertEquals("language-detector", agents.get(1).get("agentType"));
            assertEquals(
                    "value2",
                    ((Map<String, Object>) agents.get(1).get("configuration")).get("property"));

            Topic inputTopic = (Topic) defaultAgentNode.getInputConnectionImplementation();
            assertEquals("input-topic", inputTopic.topicName());
            Topic outputTopic = (Topic) defaultAgentNode.getOutputConnectionImplementation();
            assertEquals("agent-step2-input", outputTopic.topicName());

            AgentNode agentImplementationDrop =
                    implementation.getAgentImplementation(module, "step2");
            DefaultAgentNode defaultAgentNodeDrop = (DefaultAgentNode) agentImplementationDrop;
            Map<String, Object> configurationDrop = defaultAgentNodeDrop.getConfiguration();
            assertNotNull(configurationDrop.get("steps"));
            assertEquals("drop", defaultAgentNodeDrop.getAgentType());

            Topic inputTopicDrop = (Topic) defaultAgentNodeDrop.getInputConnectionImplementation();
            assertEquals("agent-step2-input", inputTopicDrop.topicName());
            Topic outputTopicDrop =
                    (Topic) defaultAgentNodeDrop.getOutputConnectionImplementation();
            assertEquals("agent-step3-input", outputTopicDrop.topicName());

            AgentNode agentImplementationLast =
                    implementation.getAgentImplementation(module, "step3");
            DefaultAgentNode defaultAgentNodeLast = (DefaultAgentNode) agentImplementationLast;
            Map<String, Object> configurationLast = defaultAgentNodeLast.getConfiguration();
            assertEquals(List.of("en"), configurationLast.get("allowedLanguages"));

            Topic inputTopicLast = (Topic) defaultAgentNodeLast.getInputConnectionImplementation();
            assertEquals("agent-step3-input", inputTopicLast.topicName());
            Topic outputTopicLast =
                    (Topic) defaultAgentNodeLast.getOutputConnectionImplementation();
            assertEquals("output-topic", outputTopicLast.topicName());
        }
    }

    @Test
    public void testMergeSourceWithProcessors() throws Exception {
        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
                                        "module.yaml",
                                        """
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
                                        idle-time: 10
                                  - name: "text-extractor"
                                    id: "step1"
                                    type: "text-extractor"
                                  - name: "language-detector"
                                    id: "step2"
                                    type: "language-detector"
                                    configuration:
                                      property: "value2"
                                  - name: "language-detector-2"
                                    id: "step3"
                                    type: "language-detector"
                                    output: "output-topic"
                                    configuration:
                                      allowedLanguages: ["en"]
                                """),
                                buildInstanceYaml(),
                                null)
                        .getApplication();

        try (ApplicationDeployer deployer =
                ApplicationDeployer.builder()
                        .registry(new ClusterRuntimeRegistry())
                        .pluginsRegistry(new PluginsRegistry())
                        .build()) {

            ExecutionPlan implementation =
                    deployer.createImplementation("app", applicationInstance);

            Module module = applicationInstance.getModule("module-1");
            assertEquals(1, implementation.getAgents().size());
            assertEquals(1, implementation.getTopics().size());
            assertTrue(
                    implementation.getConnectionImplementation(
                                    module,
                                    Connection.fromTopic(TopicDefinition.fromName("output-topic")))
                            instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);

            assertEquals(1, implementation.getAgents().size());
            AgentNode agentImplementation =
                    implementation.getAgentImplementation(module, "source-1");
            DefaultAgentNode defaultAgentNode = (DefaultAgentNode) agentImplementation;
            Map<String, Object> configuration = defaultAgentNode.getConfiguration();

            Map<String, Object> source = (Map<String, Object>) configuration.get("source");
            assertEquals("s3-source", source.get("agentType"));
            assertEquals(10, ((Map<String, Object>) source.get("configuration")).get("idle-time"));

            List<Map<String, Object>> processors =
                    (List<Map<String, Object>>) configuration.get("processors");
            assertEquals(3, processors.size());
            assertEquals("text-extractor", processors.get(0).get("agentType"));
            assertEquals("language-detector", processors.get(1).get("agentType"));
            assertEquals(
                    "value2",
                    ((Map<String, Object>) processors.get(1).get("configuration")).get("property"));
            assertEquals("language-detector", processors.get(2).get("agentType"));
            assertEquals(
                    List.of("en"),
                    ((Map<String, Object>) processors.get(2).get("configuration"))
                            .get("allowedLanguages"));

            Topic outputTopic = (Topic) defaultAgentNode.getOutputConnectionImplementation();
            assertEquals("output-topic", outputTopic.topicName());

            assertNull(defaultAgentNode.getInputConnectionImplementation());
        }
    }

    @Test
    public void testMergeSinkWithProcessorAgents() throws Exception {
        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
                                        "module.yaml",
                                        """
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
                                  - name: "language-detector"
                                    id: "step2"
                                    type: "language-detector"
                                    configuration:
                                      property: "value2"
                                  - name: "language-detector-2"
                                    id: "step3"
                                    type: "language-detector"
                                    configuration:
                                      allowedLanguages: ["en"]
                                  - name: "generic-composable-sink"
                                    type: "generic-composable-sink"
                                    configuration:
                                      paramSink: "sink1param"
                                """),
                                buildInstanceYaml(),
                                null)
                        .getApplication();

        try (ApplicationDeployer deployer =
                ApplicationDeployer.builder()
                        .registry(new ClusterRuntimeRegistry())
                        .pluginsRegistry(new PluginsRegistry())
                        .build()) {

            ExecutionPlan implementation =
                    deployer.createImplementation("app", applicationInstance);

            Module module = applicationInstance.getModule("module-1");
            assertEquals(1, implementation.getAgents().size());
            assertEquals(1, implementation.getTopics().size());
            assertTrue(
                    implementation.getConnectionImplementation(
                                    module,
                                    Connection.fromTopic(TopicDefinition.fromName("input-topic")))
                            instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);

            assertEquals(1, implementation.getAgents().size());
            AgentNode agentImplementation = implementation.getAgentImplementation(module, "step1");
            DefaultAgentNode defaultAgentNode = (DefaultAgentNode) agentImplementation;
            Map<String, Object> configuration = defaultAgentNode.getConfiguration();
            List<Map<String, Object>> processors =
                    (List<Map<String, Object>>) configuration.get("processors");
            assertEquals(3, processors.size());
            assertEquals("text-extractor", processors.get(0).get("agentType"));
            assertEquals("language-detector", processors.get(1).get("agentType"));
            assertEquals(
                    "value2",
                    ((Map<String, Object>) processors.get(1).get("configuration")).get("property"));
            assertEquals("language-detector", processors.get(2).get("agentType"));
            assertEquals(
                    List.of("en"),
                    ((Map<String, Object>) processors.get(2).get("configuration"))
                            .get("allowedLanguages"));

            Topic inputTopic = (Topic) defaultAgentNode.getInputConnectionImplementation();
            assertEquals("input-topic", inputTopic.topicName());
            assertNull(defaultAgentNode.getOutputConnectionImplementation());

            Map<String, Object> sink = (Map<String, Object>) configuration.get("sink");
            assertEquals("generic-composable-sink", sink.get("agentType"));
            assertEquals(
                    "sink1param",
                    ((Map<String, Object>) sink.get("configuration")).get("paramSink"));
        }
    }

    @Test
    public void testMergeSourceAndSinkWithProcessorAgents() throws Exception {
        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
                                        "module.yaml",
                                        """
                                module: "module-1"
                                id: "pipeline-1"
                                pipeline:
                                  - name: "the-source"
                                    id: "source-1"
                                    type: "s3-source"
                                    configuration:
                                        idle-time: 10
                                  - name: "text-extractor"
                                    id: "step1"
                                    type: "text-extractor"
                                  - name: "language-detector"
                                    id: "step2"
                                    type: "language-detector"
                                    configuration:
                                      property: "value2"
                                  - name: "language-detector-2"
                                    id: "step3"
                                    type: "language-detector"
                                    configuration:
                                      allowedLanguages: ["en"]
                                  - name: "generic-composable-sink"
                                    type: "generic-composable-sink"
                                    configuration:
                                      paramSink: "sink1param"
                                """),
                                buildInstanceYaml(),
                                null)
                        .getApplication();

        try (ApplicationDeployer deployer =
                ApplicationDeployer.builder()
                        .registry(new ClusterRuntimeRegistry())
                        .pluginsRegistry(new PluginsRegistry())
                        .build()) {

            ExecutionPlan implementation =
                    deployer.createImplementation("app", applicationInstance);

            Module module = applicationInstance.getModule("module-1");
            assertEquals(1, implementation.getAgents().size());
            assertEquals(0, implementation.getTopics().size());

            assertEquals(1, implementation.getAgents().size());
            AgentNode agentImplementation =
                    implementation.getAgentImplementation(module, "source-1");
            DefaultAgentNode defaultAgentNode = (DefaultAgentNode) agentImplementation;
            Map<String, Object> configuration = defaultAgentNode.getConfiguration();

            Map<String, Object> source = (Map<String, Object>) configuration.get("source");
            assertEquals("s3-source", source.get("agentType"));
            assertEquals(10, ((Map<String, Object>) source.get("configuration")).get("idle-time"));

            List<Map<String, Object>> processors =
                    (List<Map<String, Object>>) configuration.get("processors");
            assertEquals(3, processors.size());
            assertEquals("text-extractor", processors.get(0).get("agentType"));
            assertEquals("language-detector", processors.get(1).get("agentType"));
            assertEquals(
                    "value2",
                    ((Map<String, Object>) processors.get(1).get("configuration")).get("property"));
            assertEquals("language-detector", processors.get(2).get("agentType"));
            assertEquals(
                    List.of("en"),
                    ((Map<String, Object>) processors.get(2).get("configuration"))
                            .get("allowedLanguages"));

            assertNull(defaultAgentNode.getInputConnectionImplementation());
            assertNull(defaultAgentNode.getOutputConnectionImplementation());

            Map<String, Object> sink = (Map<String, Object>) configuration.get("sink");
            assertEquals("generic-composable-sink", sink.get("agentType"));
            assertEquals(
                    "sink1param",
                    ((Map<String, Object>) sink.get("configuration")).get("paramSink"));
        }
    }

    @Test
    public void testMergeSourceAndSinkWithProcessorAgentsWithIntermediateTopics() throws Exception {
        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
                                        "module.yaml",
                                        """
                                module: "module-1"
                                id: "pipeline-1"
                                pipeline:
                                  - name: "the-source"
                                    id: "source-1"
                                    type: "s3-source"
                                    configuration:
                                        idle-time: 10
                                  - name: "text-extractor"
                                    id: "step1"
                                    type: "text-extractor"
                                  - name: "language-detector"
                                    id: "step2"
                                    type: "language-detector"
                                    configuration:
                                      property: "value2"
                                  - name: "requires-buffer-topic"
                                    id: "bad-step"
                                    type: "drop"
                                    configuration:
                                      composable: false
                                  - name: "language-detector-2"
                                    id: "step3"
                                    type: "language-detector"
                                    configuration:
                                      allowedLanguages: ["en"]
                                  - name: "generic-composable-sink"
                                    type: "generic-composable-sink"
                                    configuration:
                                      paramSink: "sink1param"
                                """),
                                buildInstanceYaml(),
                                null)
                        .getApplication();

        try (ApplicationDeployer deployer =
                ApplicationDeployer.builder()
                        .registry(new ClusterRuntimeRegistry())
                        .pluginsRegistry(new PluginsRegistry())
                        .build()) {

            ExecutionPlan implementation =
                    deployer.createImplementation("app", applicationInstance);

            Module module = applicationInstance.getModule("module-1");

            log.info("Agents {}", implementation.getAgents().keySet());
            log.info(
                    "Topics {}",
                    implementation.getTopics().values().stream()
                            .map(Topic::topicName)
                            .collect(Collectors.toList()));

            assertEquals(3, implementation.getAgents().size());
            assertEquals(2, implementation.getTopics().size());

            assertNotNull(implementation.getTopicByName("agent-step3-input"));
            assertNotNull(implementation.getTopicByName("agent-bad-step-input"));

            AgentNode firstNode = implementation.getAgentImplementation(module, "source-1");
            DefaultAgentNode defaultFirstNode = (DefaultAgentNode) firstNode;
            Map<String, Object> configuration = defaultFirstNode.getConfiguration();

            Map<String, Object> source = (Map<String, Object>) configuration.get("source");
            assertEquals("s3-source", source.get("agentType"));
            assertEquals(10, ((Map<String, Object>) source.get("configuration")).get("idle-time"));

            List<Map<String, Object>> processors =
                    (List<Map<String, Object>>) configuration.get("processors");
            assertEquals(2, processors.size());
            assertEquals("text-extractor", processors.get(0).get("agentType"));
            assertEquals("language-detector", processors.get(1).get("agentType"));
            assertEquals(
                    "value2",
                    ((Map<String, Object>) processors.get(1).get("configuration")).get("property"));

            Topic outputTopic = (Topic) defaultFirstNode.getOutputConnectionImplementation();
            assertEquals("agent-bad-step-input", outputTopic.topicName());
            assertNull(defaultFirstNode.getInputConnectionImplementation());

            AgentNode secondNode = implementation.getAgentImplementation(module, "bad-step");
            DefaultAgentNode defaultSecondNode = (DefaultAgentNode) secondNode;
            assertEquals("drop", defaultSecondNode.getAgentType());

            Topic inputTopicSecondStep =
                    (Topic) defaultSecondNode.getInputConnectionImplementation();
            assertEquals("agent-bad-step-input", inputTopicSecondStep.topicName());
            Topic outputTopicSecondStep =
                    (Topic) defaultSecondNode.getOutputConnectionImplementation();
            assertEquals("agent-step3-input", outputTopicSecondStep.topicName());

            AgentNode thirdNode = implementation.getAgentImplementation(module, "step3");
            DefaultAgentNode defaultThirdNode = (DefaultAgentNode) thirdNode;
            Map<String, Object> configurationThirdNode = defaultThirdNode.getConfiguration();
            assertEquals("composite-agent", defaultThirdNode.getAgentType());

            processors = (List<Map<String, Object>>) configurationThirdNode.get("processors");
            assertEquals(1, processors.size());
            assertEquals("language-detector", processors.get(0).get("agentType"));
            assertEquals(
                    List.of("en"),
                    ((Map<String, Object>) processors.get(0).get("configuration"))
                            .get("allowedLanguages"));

            Map<String, Object> sink = (Map<String, Object>) configurationThirdNode.get("sink");
            assertEquals("generic-composable-sink", sink.get("agentType"));
            assertEquals(
                    "sink1param",
                    ((Map<String, Object>) sink.get("configuration")).get("paramSink"));

            Topic inputTopic = (Topic) defaultThirdNode.getInputConnectionImplementation();
            assertEquals("agent-step3-input", inputTopic.topicName());
            assertNull(defaultThirdNode.getOutputConnectionImplementation());
        }
    }

    @Test
    public void testMixDifferentResourceRequests() throws Exception {
        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
                                        "module.yaml",
                                        """
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
                                  - name: "language-detector"
                                    id: "step1b"
                                    type: "language-detector"
                                  - name: "drop-if-not-english"
                                    id: "step2"
                                    type: "drop"
                                    resources:
                                      size: 2
                                  - name: "language-detector-2"
                                    id: "step3"
                                    type: "language-detector"
                                    output: "output-topic"
                                """),
                                buildInstanceYaml(),
                                null)
                        .getApplication();

        try (ApplicationDeployer deployer =
                ApplicationDeployer.builder()
                        .registry(new ClusterRuntimeRegistry())
                        .pluginsRegistry(new PluginsRegistry())
                        .build()) {

            ExecutionPlan implementation =
                    deployer.createImplementation("app", applicationInstance);

            Module module = applicationInstance.getModule("module-1");
            assertEquals(3, implementation.getAgents().size());
            log.info(
                    "Topics {}",
                    implementation.getTopics().values().stream()
                            .map(Topic::topicName)
                            .collect(Collectors.toList()));
            assertEquals(4, implementation.getTopics().size());
            assertTrue(
                    implementation.getConnectionImplementation(
                                    module,
                                    Connection.fromTopic(TopicDefinition.fromName("input-topic")))
                            instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);
            assertTrue(
                    implementation.getConnectionImplementation(
                                    module,
                                    Connection.fromTopic(TopicDefinition.fromName("output-topic")))
                            instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);
            assertTrue(
                    implementation.getConnectionImplementation(
                                    module,
                                    Connection.fromTopic(
                                            TopicDefinition.fromName("agent-step2-input")))
                            instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);
            assertTrue(
                    implementation.getConnectionImplementation(
                                    module,
                                    Connection.fromTopic(
                                            TopicDefinition.fromName("agent-step3-input")))
                            instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);

            assertEquals(3, implementation.getAgents().size());

            AgentNode agentImplementation = implementation.getAgentImplementation(module, "step1");
            DefaultAgentNode defaultAgentNode = (DefaultAgentNode) agentImplementation;
            AbstractCompositeAgentProvider.getProcessorConfigurationAt(
                    defaultAgentNode, 0, "text-extractor");
            AbstractCompositeAgentProvider.getProcessorConfigurationAt(
                    defaultAgentNode, 1, "language-detector");

            Topic inputTopic = (Topic) defaultAgentNode.getInputConnectionImplementation();
            assertEquals("input-topic", inputTopic.topicName());
            Topic outputTopic = (Topic) defaultAgentNode.getOutputConnectionImplementation();
            assertEquals("agent-step2-input", outputTopic.topicName());

            AgentNode agentImplementationDrop =
                    implementation.getAgentImplementation(module, "step2");
            DefaultAgentNode defaultAgentNodeDrop = (DefaultAgentNode) agentImplementationDrop;
            Map<String, Object> configurationDrop = defaultAgentNodeDrop.getConfiguration();
            assertNotNull(configurationDrop.get("steps"));
            assertEquals("drop", defaultAgentNodeDrop.getAgentType());

            Topic inputTopicDrop = (Topic) defaultAgentNodeDrop.getInputConnectionImplementation();
            assertEquals("agent-step2-input", inputTopicDrop.topicName());
            Topic outputTopicDrop =
                    (Topic) defaultAgentNodeDrop.getOutputConnectionImplementation();
            assertEquals("agent-step3-input", outputTopicDrop.topicName());

            AgentNode agentImplementationLast =
                    implementation.getAgentImplementation(module, "step3");
            DefaultAgentNode defaultAgentNodeLast = (DefaultAgentNode) agentImplementationLast;
            assertEquals("language-detector", defaultAgentNodeLast.getAgentType());

            Topic inputTopicLast = (Topic) defaultAgentNodeLast.getInputConnectionImplementation();
            assertEquals("agent-step3-input", inputTopicLast.topicName());
            Topic outputTopicLast =
                    (Topic) defaultAgentNodeLast.getOutputConnectionImplementation();
            assertEquals("output-topic", outputTopicLast.topicName());
        }
    }

    @Test
    public void testMixDifferentErrorSpecs() throws Exception {
        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
                                        "module.yaml",
                                        """
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
                                  - name: "language-detector"
                                    id: "step1b"
                                    type: "language-detector"
                                  - name: "drop-if-not-english"
                                    id: "step2"
                                    type: "drop"
                                    errors:
                                      on-failure: dead-letter
                                  - name: "language-detector-2"
                                    id: "step3"
                                    type: "language-detector"
                                    output: "output-topic"
                                """),
                                buildInstanceYaml(),
                                null)
                        .getApplication();

        try (ApplicationDeployer deployer =
                ApplicationDeployer.builder()
                        .registry(new ClusterRuntimeRegistry())
                        .pluginsRegistry(new PluginsRegistry())
                        .build()) {

            ExecutionPlan implementation =
                    deployer.createImplementation("app", applicationInstance);

            Module module = applicationInstance.getModule("module-1");
            assertEquals(3, implementation.getAgents().size());
            log.info(
                    "Topics {}",
                    implementation.getTopics().values().stream()
                            .map(Topic::topicName)
                            .collect(Collectors.toList()));
            assertEquals(5, implementation.getTopics().size());
            assertTrue(
                    implementation.getConnectionImplementation(
                                    module,
                                    Connection.fromTopic(TopicDefinition.fromName("input-topic")))
                            instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);
            assertTrue(
                    implementation.getConnectionImplementation(
                                    module,
                                    Connection.fromTopic(TopicDefinition.fromName("output-topic")))
                            instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);
            assertTrue(
                    implementation.getConnectionImplementation(
                                    module,
                                    Connection.fromTopic(
                                            TopicDefinition.fromName("agent-step2-input")))
                            instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);
            assertTrue(
                    implementation.getConnectionImplementation(
                                    module,
                                    Connection.fromTopic(
                                            TopicDefinition.fromName(
                                                    "agent-step2-input-deadletter")))
                            instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);
            assertTrue(
                    implementation.getConnectionImplementation(
                                    module,
                                    Connection.fromTopic(
                                            TopicDefinition.fromName("agent-step3-input")))
                            instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);

            assertEquals(3, implementation.getAgents().size());

            AgentNode agentImplementation = implementation.getAgentImplementation(module, "step1");
            DefaultAgentNode defaultAgentNode = (DefaultAgentNode) agentImplementation;
            AbstractCompositeAgentProvider.getProcessorConfigurationAt(
                    defaultAgentNode, 0, "text-extractor");
            AbstractCompositeAgentProvider.getProcessorConfigurationAt(
                    defaultAgentNode, 1, "language-detector");

            Topic inputTopic = (Topic) defaultAgentNode.getInputConnectionImplementation();
            assertEquals("input-topic", inputTopic.topicName());
            Topic outputTopic = (Topic) defaultAgentNode.getOutputConnectionImplementation();
            assertEquals("agent-step2-input", outputTopic.topicName());

            AgentNode agentImplementationDrop =
                    implementation.getAgentImplementation(module, "step2");
            DefaultAgentNode defaultAgentNodeDrop = (DefaultAgentNode) agentImplementationDrop;
            Map<String, Object> configurationDrop = defaultAgentNodeDrop.getConfiguration();
            assertNotNull(configurationDrop.get("steps"));
            assertEquals("drop", defaultAgentNodeDrop.getAgentType());

            Topic inputTopicDrop = (Topic) defaultAgentNodeDrop.getInputConnectionImplementation();
            assertEquals("agent-step2-input", inputTopicDrop.topicName());
            Topic outputTopicDrop =
                    (Topic) defaultAgentNodeDrop.getOutputConnectionImplementation();
            assertEquals("agent-step3-input", outputTopicDrop.topicName());

            AgentNode agentImplementationLast =
                    implementation.getAgentImplementation(module, "step3");
            DefaultAgentNode defaultAgentNodeLast = (DefaultAgentNode) agentImplementationLast;
            assertEquals("language-detector", defaultAgentNodeLast.getAgentType());

            Topic inputTopicLast = (Topic) defaultAgentNodeLast.getInputConnectionImplementation();
            assertEquals("agent-step3-input", inputTopicLast.topicName());
            Topic outputTopicLast =
                    (Topic) defaultAgentNodeLast.getOutputConnectionImplementation();
            assertEquals("output-topic", outputTopicLast.topicName());
        }
    }
}
