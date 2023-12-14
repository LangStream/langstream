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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.langstream.api.model.Application;
import ai.langstream.api.model.Connection;
import ai.langstream.api.model.Module;
import ai.langstream.api.model.TopicDefinition;
import ai.langstream.api.runtime.AgentNode;
import ai.langstream.api.runtime.ClusterRuntimeRegistry;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.impl.agents.AbstractCompositeAgentProvider;
import ai.langstream.impl.common.DefaultAgentNode;
import ai.langstream.impl.deploy.ApplicationDeployer;
import ai.langstream.impl.noop.NoOpStreamingClusterRuntimeProvider;
import ai.langstream.impl.parser.ModelBuilder;
import java.util.List;
import java.util.Map;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

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
        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
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
                                  - name: "compute-embeddings"
                                    id: "step1"
                                    type: "compute-ai-embeddings"
                                    input: "input-topic"
                                    output: "output-topic"
                                    configuration:
                                      model: "text-embedding-ada-002"
                                      embeddings-field: "value.embeddings"
                                      text: "{{ value.name }} {{ value.description }}"
                                """),
                                buildInstanceYaml(),
                                null)
                        .getApplication();

        try (ApplicationDeployer deployer =
                ApplicationDeployer.builder()
                        .registry(new ClusterRuntimeRegistry())
                        .pluginsRegistry(new PluginsRegistry())
                        .build()) {

            Module module = applicationInstance.getModule("module-1");

            ExecutionPlan implementation =
                    deployer.createImplementation("app", applicationInstance);
            assertEquals(1, implementation.getAgents().size());
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

            AgentNode agentImplementation = implementation.getAgentImplementation(module, "step1");
            assertNotNull(agentImplementation);
            DefaultAgentNode step = (DefaultAgentNode) agentImplementation;
            Map<String, Object> configuration = step.getConfiguration();
            log.info("Configuration: {}", configuration);
            Map<String, Object> openAIConfiguration =
                    (Map<String, Object>) configuration.get("openai");
            log.info("openAIConfiguration: {}", openAIConfiguration);
            assertEquals("http://something", openAIConfiguration.get("url"));
            assertEquals("xxcxcxc", openAIConfiguration.get("access-key"));
            assertEquals("azure", openAIConfiguration.get("provider"));

            List<Map<String, Object>> steps =
                    (List<Map<String, Object>>) configuration.get("steps");
            assertEquals(1, steps.size());
            Map<String, Object> step1 = steps.get(0);
            assertEquals("text-embedding-ada-002", step1.get("model"));
            assertEquals("value.embeddings", step1.get("embeddings-field"));
            assertEquals("{{ value.name }} {{ value.description }}", step1.get("text"));
        }
    }

    @Test
    public void testMergeGenAIToolKitAgents() throws Exception {
        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
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
                                  - name: "compute-embeddings"
                                    id: "step1"
                                    type: "compute-ai-embeddings"
                                    input: "input-topic"
                                    configuration:
                                      model: "text-embedding-ada-002"
                                      embeddings-field: "value.embeddings"
                                      text: "{{ value.name }} {{ value.description }}"
                                  - name: "drop"
                                    id: "step2"
                                    type: "drop-fields"
                                    output: "output-topic"
                                    configuration:
                                      fields:
                                      - "embeddings"
                                      part: "value"
                                """),
                                buildInstanceYaml(),
                                null)
                        .getApplication();

        @Cleanup
        ApplicationDeployer deployer =
                ApplicationDeployer.builder()
                        .registry(new ClusterRuntimeRegistry())
                        .pluginsRegistry(new PluginsRegistry())
                        .build();

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation("app", applicationInstance);
        log.info("Agents: {}", implementation.getAgents());
        assertEquals(1, implementation.getAgents().size());

        AgentNode agentImplementation = implementation.getAgentImplementation(module, "step1");
        assertNotNull(agentImplementation);
        DefaultAgentNode step = (DefaultAgentNode) agentImplementation;

        assertEquals(AbstractCompositeAgentProvider.AGENT_TYPE, step.getAgentType());

        Map<String, Object> configuration =
                AbstractCompositeAgentProvider.getProcessorConfigurationAt(
                        step, 0, "compute-ai-embeddings");
        log.info("Configuration: {}", configuration);

        Map<String, Object> openAIConfiguration = (Map<String, Object>) configuration.get("openai");
        log.info("openAIConfiguration: {}", openAIConfiguration);
        assertEquals("http://something", openAIConfiguration.get("url"));
        assertEquals("xxcxcxc", openAIConfiguration.get("access-key"));
        assertEquals("azure", openAIConfiguration.get("provider"));

        List<Map<String, Object>> steps = (List<Map<String, Object>>) configuration.get("steps");
        assertEquals(1, steps.size());
        Map<String, Object> step1 = steps.get(0);
        assertEquals("compute-ai-embeddings", step1.get("type"));
        assertEquals("text-embedding-ada-002", step1.get("model"));
        assertEquals("value.embeddings", step1.get("embeddings-field"));
        assertEquals("{{ value.name }} {{ value.description }}", step1.get("text"));

        configuration =
                AbstractCompositeAgentProvider.getProcessorConfigurationAt(step, 1, "drop-fields");
        steps = (List<Map<String, Object>>) configuration.get("steps");
        assertEquals(1, steps.size());
        Map<String, Object> step2 = steps.get(0);
        assertEquals("drop-fields", step2.get("type"));
        assertEquals(List.of("embeddings"), step2.get("fields"));
        assertEquals("value", step2.get("part"));

        // verify that the intermediate topic is not created
        log.info(
                "topics {}",
                implementation.getTopics().keySet().stream()
                        .map(TopicDefinition::getName)
                        .toList());
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
        assertEquals(2, implementation.getTopics().size());
    }

    @Test
    public void testDontMergeGenAIToolKitAgentsWithExplicitLogTopic() throws Exception {
        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
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
                                        "module.yaml",
                                        """
                                module: "module-1"
                                id: "pipeline-1"
                                topics:
                                  - name: "log-topic"
                                    creation-mode: create-if-not-exists
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                  - name: "output-topic"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "compute-embeddings"
                                    id: "step1"
                                    type: "compute-ai-embeddings"
                                    input: "input-topic"
                                    output: "log-topic"
                                    configuration:
                                      model: "text-embedding-ada-002"
                                      embeddings-field: "value.embeddings"
                                      text: "{{ value.name }} {{ value.description }}"
                                  - name: "drop"
                                    id: "step2"
                                    type: "drop-fields"
                                    input: "log-topic"
                                    output: "output-topic"
                                    configuration:
                                      fields:
                                      - "embeddings"
                                      part: "value"
                                """),
                                buildInstanceYaml(),
                                null)
                        .getApplication();

        @Cleanup
        ApplicationDeployer deployer =
                ApplicationDeployer.builder()
                        .registry(new ClusterRuntimeRegistry())
                        .pluginsRegistry(new PluginsRegistry())
                        .build();

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation("app", applicationInstance);
        log.info("Agents: {}", implementation.getAgents());
        assertEquals(2, implementation.getAgents().size());

        {
            AgentNode agentImplementation = implementation.getAgentImplementation(module, "step1");
            assertNotNull(agentImplementation);
            DefaultAgentNode step = (DefaultAgentNode) agentImplementation;
            Map<String, Object> configuration = step.getConfiguration();
            log.info("Configuration: {}", configuration);
            Map<String, Object> openAIConfiguration =
                    (Map<String, Object>) configuration.get("openai");
            log.info("openAIConfiguration: {}", openAIConfiguration);
            assertEquals("http://something", openAIConfiguration.get("url"));
            assertEquals("xxcxcxc", openAIConfiguration.get("access-key"));
            assertEquals("azure", openAIConfiguration.get("provider"));

            List<Map<String, Object>> steps =
                    (List<Map<String, Object>>) configuration.get("steps");
            assertEquals(1, steps.size());
            Map<String, Object> step1 = steps.get(0);
            assertEquals("compute-ai-embeddings", step1.get("type"));
            assertEquals("text-embedding-ada-002", step1.get("model"));
            assertEquals("value.embeddings", step1.get("embeddings-field"));
            assertEquals("{{ value.name }} {{ value.description }}", step1.get("text"));
        }

        {
            AgentNode agentImplementation = implementation.getAgentImplementation(module, "step2");
            assertNotNull(agentImplementation);
            DefaultAgentNode step = (DefaultAgentNode) agentImplementation;
            Map<String, Object> configuration = step.getConfiguration();
            log.info("Configuration: {}", configuration);
            assertNull(configuration.get("openai"));
            List<Map<String, Object>> steps =
                    (List<Map<String, Object>>) configuration.get("steps");
            assertEquals(1, steps.size());
            Map<String, Object> step2 = steps.get(0);
            assertEquals("drop-fields", step2.get("type"));
            assertEquals(List.of("embeddings"), step2.get("fields"));
            assertEquals("value", step2.get("part"));
        }

        // verify that the intermediate topic is not created
        log.info(
                "topics {}",
                implementation.getTopics().keySet().stream()
                        .map(TopicDefinition::getName)
                        .toList());
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
                                module, Connection.fromTopic(TopicDefinition.fromName("log-topic")))
                        instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);
        assertEquals(3, implementation.getTopics().size());
    }

    @Test
    public void testMapAllGenAIToolKitAgents() throws Exception {
        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
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
                                        service: jdbc
                                        url: "jdbc:postgresql://localhost:5432/postgres"
                                        driverClass: "org.postgresql.Driver"
                                  """,
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
                                  - name: "compute-embeddings"
                                    id: step1
                                    type: "compute-ai-embeddings"
                                    input: "input-topic"
                                    configuration:
                                      model: "text-embedding-ada-002"
                                      embeddings-field: "value.embeddings"
                                      text: "{{ value.name }} {{ value.description }}"
                                  - name: "dropfields"
                                    type: "drop-fields"
                                    configuration:
                                      fields:
                                      - "embeddings"
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
                                """),
                                buildInstanceYaml(),
                                null)
                        .getApplication();

        @Cleanup
        ApplicationDeployer deployer =
                ApplicationDeployer.builder()
                        .registry(new ClusterRuntimeRegistry())
                        .pluginsRegistry(new PluginsRegistry())
                        .build();

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation("app", applicationInstance);
        log.info("Agents: {}", implementation.getAgents());
        assertEquals(1, implementation.getAgents().size());

        AgentNode agentImplementation = implementation.getAgentImplementation(module, "step1");
        assertNotNull(agentImplementation);
        DefaultAgentNode step = (DefaultAgentNode) agentImplementation;

        Map<String, Object> configuration =
                AbstractCompositeAgentProvider.getProcessorConfigurationAt(
                        step, 0, "compute-ai-embeddings");
        log.info("Configuration: {}", configuration);
        Map<String, Object> openAIConfiguration = (Map<String, Object>) configuration.get("openai");
        log.info("openAIConfiguration: {}", openAIConfiguration);
        assertEquals("http://something", openAIConfiguration.get("url"));
        assertEquals("xxcxcxc", openAIConfiguration.get("access-key"));
        assertEquals("azure", openAIConfiguration.get("provider"));
        List<Map<String, Object>> steps = (List<Map<String, Object>>) configuration.get("steps");
        assertEquals(1, steps.size());

        AbstractCompositeAgentProvider.getProcessorConfigurationAt(step, 1, "drop-fields");
        AbstractCompositeAgentProvider.getProcessorConfigurationAt(step, 2, "drop");
        AbstractCompositeAgentProvider.getProcessorConfigurationAt(step, 3, "query");
        AbstractCompositeAgentProvider.getProcessorConfigurationAt(step, 4, "unwrap-key-value");
        AbstractCompositeAgentProvider.getProcessorConfigurationAt(step, 5, "flatten");
        AbstractCompositeAgentProvider.getProcessorConfigurationAt(step, 6, "compute");
        AbstractCompositeAgentProvider.getProcessorConfigurationAt(step, 7, "merge-key-value");
        AbstractCompositeAgentProvider.getProcessorConfigurationAt(step, 8, "ai-chat-completions");
        AbstractCompositeAgentProvider.getProcessorConfigurationAt(step, 9, "cast");

        // verify that the intermediate topics are not created
        log.info("topics {}", implementation.getTopics());
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
    }

    @Test
    public void testMultipleQuerySteps() throws Exception {
        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
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
                                        service: jdbc
                                        url: "jdbc:postgresql://localhost:5432/postgres"
                                        driverClass: "org.postgresql.Driver"
                                    - name: my-database-2
                                      type: datasource
                                      configuration:
                                        service: jdbc
                                        url: "jdbc:postgresql://localhost:5432/postgres"
                                        driverClass: "org.postgresql.Driver"
                                  """,
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
                                  - name: "query1"
                                    id: query1
                                    type: "query"
                                    configuration:
                                      composable: false
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
                                """),
                                buildInstanceYaml(),
                                null)
                        .getApplication();

        @Cleanup
        ApplicationDeployer deployer =
                ApplicationDeployer.builder()
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
            Map<String, Object> datasourceConfiguration1 =
                    (Map<String, Object>) configuration.get("datasource");
            assertEquals("jdbc", datasourceConfiguration1.get("service"));
            assertEquals(
                    "jdbc:postgresql://localhost:5432/postgres",
                    datasourceConfiguration1.get("url"));
            assertEquals("org.postgresql.Driver", datasourceConfiguration1.get("driverClass"));
            List<Map<String, Object>> steps =
                    (List<Map<String, Object>>) configuration.get("steps");
            assertEquals(1, steps.size());
        }

        {
            AgentNode agentImplementation = implementation.getAgentImplementation(module, "query2");
            DefaultAgentNode step = (DefaultAgentNode) agentImplementation;
            {
                Map<String, Object> configuration =
                        AbstractCompositeAgentProvider.getProcessorConfigurationAt(
                                step, 0, "query");
                log.info("Configuration: {}", configuration);
                Map<String, Object> datasourceConfiguration1 =
                        (Map<String, Object>) configuration.get("datasource");
                assertEquals("jdbc", datasourceConfiguration1.get("service"));
                assertEquals(
                        "jdbc:postgresql://localhost:5432/postgres",
                        datasourceConfiguration1.get("url"));
                assertEquals("org.postgresql.Driver", datasourceConfiguration1.get("driverClass"));
                List<Map<String, Object>> steps =
                        (List<Map<String, Object>>) configuration.get("steps");
                // query + cast
                assertEquals(1, steps.size());
            }

            // verify second step
            AbstractCompositeAgentProvider.getProcessorConfigurationAt(step, 1, "cast");
        }

        // verify that an intermediate topic is created
        log.info(
                "topics {}",
                implementation.getTopics().keySet().stream()
                        .map(TopicDefinition::getName)
                        .toList());
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
                                        TopicDefinition.fromName("agent-query2-input")))
                        instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);
    }

    @Test
    public void testEmbeddingsThanQuery() throws Exception {
        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
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
                                        service: jdbc
                                        url: "jdbc:postgresql://localhost:5432/postgres"
                                        driverClass: "org.postgresql.Driver"
                                  """,
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
                                  - name: "compute-embeddings"
                                    id: "step1"
                                    type: "compute-ai-embeddings"
                                    input: "input-topic"
                                    configuration:
                                      model: "text-embedding-ada-002"
                                      embeddings-field: "value.embeddings"
                                      text: "{{ value.name }} {{ value.description }}"
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
                                    id: casttojson
                                    type: "cast"
                                    output: "output-topic"
                                    configuration:
                                      schema-type: "string"
                                """),
                                buildInstanceYaml(),
                                null)
                        .getApplication();

        @Cleanup
        ApplicationDeployer deployer =
                ApplicationDeployer.builder()
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

            Map<String, Object> computeAiConfiguration =
                    AbstractCompositeAgentProvider.getProcessorConfigurationAt(
                            step, 0, "compute-ai-embeddings");
            assertNotNull(computeAiConfiguration.get("openai"));

            Map<String, Object> configuration =
                    AbstractCompositeAgentProvider.getProcessorConfigurationAt(step, 1, "query");
            log.info("Configuration: {}", configuration);
            Map<String, Object> datasourceConfiguration1 =
                    (Map<String, Object>) configuration.get("datasource");
            assertEquals("jdbc", datasourceConfiguration1.get("service"));
            assertEquals(
                    "jdbc:postgresql://localhost:5432/postgres",
                    datasourceConfiguration1.get("url"));
            assertEquals("org.postgresql.Driver", datasourceConfiguration1.get("driverClass"));
            List<Map<String, Object>> steps =
                    (List<Map<String, Object>>) configuration.get("steps");
            assertEquals(1, steps.size());

            AbstractCompositeAgentProvider.getProcessorConfigurationAt(step, 2, "cast");
        }
    }

    @Test
    public void testForceAiService() throws Exception {
        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
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
                                    - name: my-vertex
                                      type: vertex-configuration
                                      configuration:
                                        url: "http://something"
                                        token: xx
                                        project: yy
                                        region: us-central1
                                  """,
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
                                  - name: "compute-embeddings"
                                    id: "step1"
                                    type: "compute-ai-embeddings"
                                    input: "input-topic"
                                    output: "output-topic"
                                    configuration:
                                      ai-service: "my-vertex"
                                      model: "text-embedding-ada-002"
                                      embeddings-field: "value.embeddings"
                                      text: "{{ value.name }} {{ value.description }}"
                                """),
                                buildInstanceYaml(),
                                null)
                        .getApplication();

        try (ApplicationDeployer deployer =
                ApplicationDeployer.builder()
                        .registry(new ClusterRuntimeRegistry())
                        .pluginsRegistry(new PluginsRegistry())
                        .build()) {

            Module module = applicationInstance.getModule("module-1");

            ExecutionPlan implementation =
                    deployer.createImplementation("app", applicationInstance);
            assertEquals(1, implementation.getAgents().size());
            AgentNode agentImplementation = implementation.getAgentImplementation(module, "step1");
            assertNotNull(agentImplementation);
            DefaultAgentNode step = (DefaultAgentNode) agentImplementation;
            Map<String, Object> configuration = step.getConfiguration();
            log.info("Configuration: {}", configuration);
            assertNull(configuration.get("openai"));
            assertNotNull(configuration.get("vertex"));
            assertNull(configuration.get("service"));
        }
    }

    @Test
    public void testValidateBadComputeStep() throws Exception {
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
                                  - name: "compute"
                                    id: "step1"
                                    type: "compute"
                                    input: "input-topic"
                                    configuration:
                                      fields:
                                         - name: value
                                           expression: "fn:concat('something', fn:len(value))"
                                """),
                                buildInstanceYaml(),
                                null)
                        .getApplication();

        try (ApplicationDeployer deployer =
                ApplicationDeployer.builder()
                        .registry(new ClusterRuntimeRegistry())
                        .pluginsRegistry(new PluginsRegistry())
                        .build()) {
            Exception e =
                    assertThrows(
                            Exception.class,
                            () -> {
                                deployer.createImplementation("app", applicationInstance);
                            });
            assertEquals("Function [fn:len] not found", e.getMessage());
        }
    }
}
