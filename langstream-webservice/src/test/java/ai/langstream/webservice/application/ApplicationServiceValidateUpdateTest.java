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
package ai.langstream.webservice.application;

import ai.langstream.api.model.Application;
import ai.langstream.api.model.ResourcesSpec;
import ai.langstream.api.model.SchemaDefinition;
import ai.langstream.api.model.TopicDefinition;
import ai.langstream.api.runtime.ClusterRuntimeRegistry;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.deployer.k8s.util.SerializationUtil;
import ai.langstream.impl.deploy.ApplicationDeployer;
import ai.langstream.impl.parser.ModelBuilder;
import ai.langstream.webservice.config.ApplicationDeployProperties;
import ai.langstream.webservice.config.TenantProperties;
import java.util.List;
import java.util.Map;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

class ApplicationServiceValidateUpdateTest {

    @Test
    void testTopics() {
        checkTopics(
                List.of(
                        new ModelBuilder.TopicDefinitionModel(
                                "input-topic", null, null, null, 0, null, null, null)),
                List.of(
                        new ModelBuilder.TopicDefinitionModel(
                                "input-topic", null, null, null, 0, null, null, null)),
                true);

        checkTopics(
                List.of(
                        new ModelBuilder.TopicDefinitionModel(
                                "input-topic", null, null, null, 0, null, null, null)),
                List.of(
                        new ModelBuilder.TopicDefinitionModel(
                                "input-topic1", null, null, null, 0, null, null, null)),
                false);

        checkTopics(
                List.of(
                        new ModelBuilder.TopicDefinitionModel(
                                "input-topic", null, null, null, 0, null, null, null)),
                List.of(
                        new ModelBuilder.TopicDefinitionModel(
                                "input-topic", null, null, null, 0, null, null, null),
                        new ModelBuilder.TopicDefinitionModel(
                                "input-topic1", null, null, null, 0, null, null, null)),
                false);

        checkTopics(
                List.of(
                        new ModelBuilder.TopicDefinitionModel(
                                "input-topic", null, null, null, 0, null, null, null),
                        new ModelBuilder.TopicDefinitionModel(
                                "input-topic1", null, null, null, 0, null, null, null)),
                List.of(
                        new ModelBuilder.TopicDefinitionModel(
                                "input-topic", null, null, null, 0, null, null, null)),
                false);

        checkTopics(
                List.of(
                        new ModelBuilder.TopicDefinitionModel(
                                "input-topic",
                                TopicDefinition.CREATE_MODE_CREATE_IF_NOT_EXISTS,
                                null,
                                null,
                                0,
                                null,
                                null,
                                null)),
                List.of(
                        new ModelBuilder.TopicDefinitionModel(
                                "input-topic",
                                TopicDefinition.CREATE_MODE_CREATE_IF_NOT_EXISTS,
                                null,
                                null,
                                0,
                                null,
                                null,
                                null)),
                true);

        checkTopics(
                List.of(
                        new ModelBuilder.TopicDefinitionModel(
                                "input-topic",
                                TopicDefinition.CREATE_MODE_NONE,
                                null,
                                null,
                                0,
                                null,
                                null,
                                null)),
                List.of(
                        new ModelBuilder.TopicDefinitionModel(
                                "input-topic", null, null, null, 0, null, null, null)),
                true);

        checkTopics(
                List.of(
                        new ModelBuilder.TopicDefinitionModel(
                                "input-topic",
                                null,
                                null,
                                new SchemaDefinition("avro", "{}", null),
                                0,
                                null,
                                null,
                                null)),
                List.of(
                        new ModelBuilder.TopicDefinitionModel(
                                "input-topic", null, null, null, 0, null, null, null)),
                false);

        checkTopics(
                List.of(
                        new ModelBuilder.TopicDefinitionModel(
                                "input-topic",
                                null,
                                null,
                                new SchemaDefinition("avro", "{}", null),
                                0,
                                null,
                                null,
                                null)),
                List.of(
                        new ModelBuilder.TopicDefinitionModel(
                                "input-topic",
                                null,
                                null,
                                new SchemaDefinition("json", "{}", null),
                                0,
                                null,
                                null,
                                null)),
                false);

        checkTopics(
                List.of(
                        new ModelBuilder.TopicDefinitionModel(
                                "input-topic",
                                null,
                                null,
                                new SchemaDefinition("avro", "{}", null),
                                0,
                                null,
                                null,
                                null)),
                List.of(
                        new ModelBuilder.TopicDefinitionModel(
                                "input-topic",
                                null,
                                null,
                                new SchemaDefinition("avro", "{schema:true}", null),
                                0,
                                null,
                                null,
                                null)),
                false);

        checkTopics(
                List.of(
                        new ModelBuilder.TopicDefinitionModel(
                                "input-topic", null, null, null, 1, null, null, null)),
                List.of(
                        new ModelBuilder.TopicDefinitionModel(
                                "input-topic", null, null, null, 0, null, null, null)),
                false);

        checkTopics(
                List.of(
                        new ModelBuilder.TopicDefinitionModel(
                                "input-topic", null, null, null, 1, null, null, null)),
                List.of(
                        new ModelBuilder.TopicDefinitionModel(
                                "input-topic", null, null, null, 2, null, null, null)),
                false);
    }

    private static void checkTopics(
            List<ModelBuilder.TopicDefinitionModel> from,
            List<ModelBuilder.TopicDefinitionModel> to,
            boolean expectValid) {
        final ApplicationService service = getApplicationService();
        try {
            service.validateTopicsUpdate(buildPlanWithTopics(from), buildPlanWithTopics(to));
            if (!expectValid) {
                throw new RuntimeException("Expected invalid topics update");
            }
        } catch (Exception e) {
            if (expectValid) {
                throw new RuntimeException(e);
            }
        }
    }

    @NotNull
    private static ApplicationService getApplicationService() {
        return new ApplicationService(
                null,
                null,
                new ApplicationDeployProperties(
                        new ApplicationDeployProperties.GatewayProperties(false)),
                new TenantProperties());
    }

    @SneakyThrows
    private static ExecutionPlan buildPlanWithTopics(
            List<ModelBuilder.TopicDefinitionModel> topics) {
        @Cleanup
        ApplicationDeployer deployer =
                ApplicationDeployer.builder()
                        .registry(new ClusterRuntimeRegistry())
                        .pluginsRegistry(new PluginsRegistry())
                        .build();

        final ModelBuilder.PipelineFileModel pipelineFileModel =
                new ModelBuilder.PipelineFileModel();
        pipelineFileModel.setId("pi");
        pipelineFileModel.setModule("mod");
        pipelineFileModel.setTopics(topics);
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
                                        SerializationUtil.writeAsYaml(pipelineFileModel)),
                                """
                        instance:
                                  streamingCluster:
                                    type: "noop"
                                  computeCluster:
                                    type: "none"
                        """,
                                null)
                        .getApplication();
        return deployer.createImplementation("app", applicationInstance);
    }

    @Test
    void testAgents() {
        checkAgents(
                List.of(
                        new ModelBuilder.AgentModel(
                                "agent",
                                "My Agent",
                                "drop",
                                "input-topic",
                                "output-topic",
                                Map.of(),
                                null,
                                null)),
                List.of(
                        new ModelBuilder.AgentModel(
                                "agent",
                                "My Agent",
                                "drop",
                                "input-topic",
                                "output-topic",
                                Map.of(),
                                null,
                                null)),
                true);

        checkAgents(
                List.of(
                        new ModelBuilder.AgentModel(
                                "agent",
                                "My Agent",
                                "drop",
                                "input-topic",
                                "output-topic",
                                Map.of(),
                                null,
                                null)),
                List.of(
                        new ModelBuilder.AgentModel(
                                "agent1",
                                "My Agent",
                                "drop",
                                "input-topic",
                                "output-topic",
                                Map.of(),
                                null,
                                null)),
                false);

        checkAgents(
                List.of(
                        new ModelBuilder.AgentModel(
                                "agent1",
                                "My Agent",
                                "drop",
                                "input-topic",
                                "output-topic",
                                Map.of(),
                                null,
                                null)),
                List.of(
                        new ModelBuilder.AgentModel(
                                "agent",
                                "My Agent",
                                "drop",
                                "input-topic",
                                "output-topic",
                                Map.of(),
                                null,
                                null)),
                false);

        checkAgents(
                List.of(
                        new ModelBuilder.AgentModel(
                                "agent",
                                "My Agent",
                                "drop",
                                "input-topic",
                                "output-topic",
                                Map.of(),
                                null,
                                null)),
                List.of(
                        new ModelBuilder.AgentModel(
                                "agent",
                                "My Agent",
                                "drop",
                                "input-topic",
                                "output-topic",
                                Map.of(),
                                null,
                                null),
                        new ModelBuilder.AgentModel(
                                "agent2",
                                "My Agent",
                                "drop",
                                "input-topic",
                                "output-topic",
                                Map.of(),
                                null,
                                null)),
                false);

        checkAgents(
                List.of(
                        new ModelBuilder.AgentModel(
                                "agent1",
                                "My Agent",
                                "drop",
                                "input-topic",
                                "output-topic",
                                Map.of(),
                                null,
                                null),
                        new ModelBuilder.AgentModel(
                                "agent2",
                                "My Agent",
                                "drop",
                                "input-topic",
                                "output-topic",
                                Map.of(),
                                null,
                                null)),
                List.of(
                        new ModelBuilder.AgentModel(
                                "agent1",
                                "My Agent",
                                "drop",
                                "input-topic",
                                "output-topic",
                                Map.of(),
                                null,
                                null)),
                false);

        checkAgents(
                List.of(
                        new ModelBuilder.AgentModel(
                                "agent",
                                "My Agent",
                                "drop",
                                "input-topic",
                                "output-topic",
                                Map.of(),
                                null,
                                null)),
                List.of(
                        new ModelBuilder.AgentModel(
                                "agent",
                                "My Agent - another name",
                                "drop",
                                "input-topic",
                                "output-topic",
                                Map.of(),
                                null,
                                null)),
                true);

        checkAgents(
                List.of(
                        new ModelBuilder.AgentModel(
                                "agent",
                                "My Agent",
                                "drop",
                                "input-topic",
                                "output-topic",
                                Map.of(),
                                null,
                                null)),
                List.of(
                        new ModelBuilder.AgentModel(
                                "agent",
                                "My Agent",
                                "drop-fields",
                                "input-topic",
                                "output-topic",
                                Map.of("fields", "f"),
                                null,
                                null)),
                false);

        checkAgents(
                List.of(
                        new ModelBuilder.AgentModel(
                                "agent",
                                "My Agent",
                                "drop",
                                "input-topic",
                                "output-topic",
                                Map.of(),
                                null,
                                null)),
                List.of(
                        new ModelBuilder.AgentModel(
                                "agent",
                                "My Agent",
                                "drop",
                                "output-topic",
                                "input-topic",
                                Map.of(),
                                null,
                                null)),
                false);

        checkAgents(
                List.of(
                        new ModelBuilder.AgentModel(
                                "agent",
                                "My Agent",
                                "drop",
                                "input-topic",
                                "output-topic",
                                Map.of("when", "true"),
                                null,
                                null)),
                List.of(
                        new ModelBuilder.AgentModel(
                                "agent",
                                "My Agent",
                                "drop",
                                "input-topic",
                                "output-topic",
                                Map.of("when", "false"),
                                null,
                                null)),
                true);

        checkAgents(
                List.of(
                        new ModelBuilder.AgentModel(
                                "agent",
                                "My Agent",
                                "drop",
                                "input-topic",
                                "output-topic",
                                Map.of("when", "true"),
                                null,
                                null)),
                List.of(
                        new ModelBuilder.AgentModel(
                                "agent",
                                "My Agent",
                                "drop",
                                "input-topic",
                                "output-topic",
                                Map.of("composable", "false"),
                                null,
                                null)),
                true);

        checkAgents(
                List.of(
                        new ModelBuilder.AgentModel(
                                "agent",
                                "My Agent",
                                "drop",
                                "input-topic",
                                "output-topic",
                                Map.of(),
                                new ResourcesSpec(1, 1, null),
                                null)),
                List.of(
                        new ModelBuilder.AgentModel(
                                "agent",
                                "My Agent",
                                "drop",
                                "input-topic",
                                "output-topic",
                                Map.of(),
                                new ResourcesSpec(1, 1, null),
                                null)),
                true);

        checkAgents(
                List.of(
                        new ModelBuilder.AgentModel(
                                "agent",
                                "My Agent",
                                "drop",
                                "input-topic",
                                "output-topic",
                                Map.of(),
                                new ResourcesSpec(1, 1, null),
                                null)),
                List.of(
                        new ModelBuilder.AgentModel(
                                "agent",
                                "My Agent",
                                "drop",
                                "input-topic",
                                "output-topic",
                                Map.of(),
                                new ResourcesSpec(2, 1, null),
                                null)),
                true);

        checkAgents(
                List.of(
                        new ModelBuilder.AgentModel(
                                "agent",
                                "My Agent",
                                "drop",
                                "input-topic",
                                "output-topic",
                                Map.of(),
                                new ResourcesSpec(1, 1, null),
                                null)),
                List.of(
                        new ModelBuilder.AgentModel(
                                "agent",
                                "My Agent",
                                "drop",
                                "input-topic",
                                "output-topic",
                                Map.of(),
                                new ResourcesSpec(1, 2, null),
                                null)),
                true);

        checkAgents(
                List.of(
                        new ModelBuilder.AgentModel(
                                "agent",
                                "My Agent",
                                "drop",
                                "input-topic",
                                "output-topic",
                                Map.of(),
                                new ResourcesSpec(1, 1, null),
                                null)),
                List.of(
                        new ModelBuilder.AgentModel(
                                "agent",
                                "My Agent",
                                "drop",
                                "input-topic",
                                "output-topic",
                                Map.of(),
                                new ResourcesSpec(2, 2, null),
                                null)),
                true);

        checkAgents(
                List.of(
                        new ModelBuilder.AgentModel(
                                "agent",
                                "My Agent",
                                "drop",
                                "input-topic",
                                "output-topic",
                                Map.of(),
                                new ResourcesSpec(2, 2, null),
                                null)),
                List.of(
                        new ModelBuilder.AgentModel(
                                "agent",
                                "My Agent",
                                "drop",
                                "input-topic",
                                "output-topic",
                                Map.of(),
                                new ResourcesSpec(1, 1, null),
                                null)),
                true);
    }

    private static void checkAgents(
            List<ModelBuilder.AgentModel> from,
            List<ModelBuilder.AgentModel> to,
            boolean expectValid) {
        final ApplicationService service = getApplicationService();
        try {
            service.validateTopicsUpdate(buildPlanWithModels(from), buildPlanWithModels(to));
            if (!expectValid) {
                throw new RuntimeException("Expected invalid topics update");
            }
        } catch (Exception e) {
            if (expectValid) {
                throw new RuntimeException(e);
            }
        }
    }

    @SneakyThrows
    private static ExecutionPlan buildPlanWithModels(List<ModelBuilder.AgentModel> agents) {
        @Cleanup
        ApplicationDeployer deployer =
                ApplicationDeployer.builder()
                        .registry(new ClusterRuntimeRegistry())
                        .pluginsRegistry(new PluginsRegistry())
                        .build();

        final ModelBuilder.PipelineFileModel pipelineFileModel =
                new ModelBuilder.PipelineFileModel();
        pipelineFileModel.setId("pi");
        pipelineFileModel.setModule("mod");
        pipelineFileModel.setTopics(
                List.of(
                        new ModelBuilder.TopicDefinitionModel(
                                "input-topic", null, null, null, 0, null, null, null),
                        new ModelBuilder.TopicDefinitionModel(
                                "output-topic", null, null, null, 0, null, null, null)));
        pipelineFileModel.setPipeline(agents);
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
                                        SerializationUtil.writeAsYaml(pipelineFileModel)),
                                """
                                instance:
                                  streamingCluster:
                                    type: "noop"
                                  computeCluster:
                                    type: "none"
                                        """,
                                null)
                        .getApplication();
        return deployer.createImplementation("app", applicationInstance);
    }
}
