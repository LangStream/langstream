package com.datastax.oss.sga.webservice.application;

import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.ResourcesSpec;
import com.datastax.oss.sga.api.model.SchemaDefinition;
import com.datastax.oss.sga.api.model.TopicDefinition;
import com.datastax.oss.sga.api.runtime.ClusterRuntimeRegistry;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.PluginsRegistry;
import com.datastax.oss.sga.deployer.k8s.util.SerializationUtil;
import com.datastax.oss.sga.impl.deploy.ApplicationDeployer;
import com.datastax.oss.sga.impl.parser.ModelBuilder;
import com.datastax.oss.sga.webservice.config.ApplicationDeployProperties;
import java.util.List;
import java.util.Map;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

class ApplicationServiceValidateUpdateTest {

    @Test
    void testTopics() throws Exception {
        checkTopics(
                List.of(new ModelBuilder.TopicDefinitionModel("input-topic", null, null, 0, null, null, null)),
                List.of(new ModelBuilder.TopicDefinitionModel("input-topic", null, null, 0, null, null, null)),
                true);

        checkTopics(
                List.of(new ModelBuilder.TopicDefinitionModel("input-topic", null, null, 0, null, null, null)),
                List.of(new ModelBuilder.TopicDefinitionModel("input-topic1", null, null, 0, null, null, null)),
                false);

        checkTopics(
                List.of(new ModelBuilder.TopicDefinitionModel("input-topic", null, null, 0, null, null, null)),
                List.of(new ModelBuilder.TopicDefinitionModel("input-topic", null, null, 0, null, null, null),
                        new ModelBuilder.TopicDefinitionModel("input-topic1", null, null, 0, null, null, null)),
                false);

        checkTopics(
                List.of(new ModelBuilder.TopicDefinitionModel("input-topic", null, null, 0, null, null, null),
                        new ModelBuilder.TopicDefinitionModel("input-topic1", null, null, 0, null, null, null)),
                List.of(new ModelBuilder.TopicDefinitionModel("input-topic", null, null, 0, null, null, null)),
                false);

        checkTopics(
                List.of(new ModelBuilder.TopicDefinitionModel("input-topic",
                        TopicDefinition.CREATE_MODE_CREATE_IF_NOT_EXISTS, null, 0,
                        null, null, null)),
                List.of(new ModelBuilder.TopicDefinitionModel("input-topic",
                        TopicDefinition.CREATE_MODE_CREATE_IF_NOT_EXISTS, null, 0,
                        null, null, null)),
                true);


        checkTopics(
                List.of(new ModelBuilder.TopicDefinitionModel("input-topic", TopicDefinition.CREATE_MODE_NONE, null, 0,
                        null, null, null)),
                List.of(new ModelBuilder.TopicDefinitionModel("input-topic", null, null, 0, null, null, null)),
                true);

        checkTopics(
                List.of(new ModelBuilder.TopicDefinitionModel("input-topic", null,
                        new SchemaDefinition("avro", "{}", null), 0, null, null, null)),
                List.of(new ModelBuilder.TopicDefinitionModel("input-topic", null, null, 0, null, null, null)),
                false);

        checkTopics(
                List.of(new ModelBuilder.TopicDefinitionModel("input-topic", null,
                        new SchemaDefinition("avro", "{}", null), 0, null, null, null)),
                List.of(new ModelBuilder.TopicDefinitionModel("input-topic", null,
                        new SchemaDefinition("json", "{}", null), 0, null, null, null)),
                false);

        checkTopics(
                List.of(new ModelBuilder.TopicDefinitionModel("input-topic", null,
                        new SchemaDefinition("avro", "{}", null), 0, null, null, null)),
                List.of(new ModelBuilder.TopicDefinitionModel("input-topic", null,
                        new SchemaDefinition("avro", "{schema:true}", null),
                        0, null, null, null)),
                false);

        checkTopics(
                List.of(new ModelBuilder.TopicDefinitionModel("input-topic", null, null, 1, null, null, null)),
                List.of(new ModelBuilder.TopicDefinitionModel("input-topic", null, null, 0, null, null, null)),
                false);

        checkTopics(
                List.of(new ModelBuilder.TopicDefinitionModel("input-topic", null, null, 1, null, null, null)),
                List.of(new ModelBuilder.TopicDefinitionModel("input-topic", null, null, 2, null, null, null)),
                false);


    }

    private static void checkTopics(List<ModelBuilder.TopicDefinitionModel> from,
                                    List<ModelBuilder.TopicDefinitionModel> to, boolean expectValid) {
        final ApplicationService service = getApplicationService();
        try {
            service.validateTopicsUpdate(
                    buildPlanWithTopics(from),
                    buildPlanWithTopics(to)
            );
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
        final ApplicationService service = new ApplicationService(null, null, new ApplicationDeployProperties(
                new ApplicationDeployProperties.GatewayProperties(false)));
        return service;
    }

    @SneakyThrows
    private static ExecutionPlan buildPlanWithTopics(List<ModelBuilder.TopicDefinitionModel> topics) {
        @Cleanup ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        final ModelBuilder.PipelineFileModel pipelineFileModel = new ModelBuilder.PipelineFileModel();
        pipelineFileModel.setId("pi");
        pipelineFileModel.setModule("mod");
        pipelineFileModel.setTopics(topics);
        Application applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of("instance.yaml",
                        """
                                instance:
                                  streamingCluster:
                                    type: "noop"     
                                  computeCluster:
                                    type: "none"         
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
                        "module.yaml", SerializationUtil.writeAsYaml(pipelineFileModel)));
        return deployer.createImplementation("app", applicationInstance);
    }

    @Test
    void testAgents() throws Exception {
        checkAgents(
                List.of(new ModelBuilder.AgentModel("agent", "My Agent", "drop", "input-topic",
                        "output-topic", Map.of(), null, null)),
                List.of(new ModelBuilder.AgentModel("agent", "My Agent", "drop", "input-topic",
                        "output-topic", Map.of(), null, null)),
                true);

        checkAgents(
                List.of(new ModelBuilder.AgentModel("agent", "My Agent", "drop", "input-topic",
                        "output-topic", Map.of(), null, null)),
                List.of(new ModelBuilder.AgentModel("agent1", "My Agent", "drop", "input-topic",
                        "output-topic", Map.of(), null, null)),
                false);

        checkAgents(
                List.of(new ModelBuilder.AgentModel("agent1", "My Agent", "drop", "input-topic",
                        "output-topic", Map.of(), null, null)),
                List.of(new ModelBuilder.AgentModel("agent", "My Agent", "drop", "input-topic",
                        "output-topic", Map.of(), null, null)),
                false);

        checkAgents(
                List.of(new ModelBuilder.AgentModel("agent", "My Agent", "drop", "input-topic",
                        "output-topic", Map.of(), null, null)),
                List.of(new ModelBuilder.AgentModel("agent", "My Agent", "drop", "input-topic",
                        "output-topic", Map.of(), null, null),
                        new ModelBuilder.AgentModel("agent2", "My Agent", "drop", "input-topic",
                                "output-topic", Map.of(), null, null)),
                false);

        checkAgents(
                List.of(new ModelBuilder.AgentModel("agent1", "My Agent", "drop", "input-topic",
                        "output-topic", Map.of(), null, null),
                        new ModelBuilder.AgentModel("agent2", "My Agent", "drop", "input-topic",
                                "output-topic", Map.of(), null, null)),
                List.of(new ModelBuilder.AgentModel("agent1", "My Agent", "drop", "input-topic",
                        "output-topic", Map.of(), null, null)),
                false);



        checkAgents(
                List.of(new ModelBuilder.AgentModel("agent", "My Agent", "drop", "input-topic",
                        "output-topic", Map.of(), null, null)),
                List.of(new ModelBuilder.AgentModel("agent", "My Agent - another name", "drop", "input-topic",
                        "output-topic", Map.of(), null, null)),
                true);

        checkAgents(
                List.of(new ModelBuilder.AgentModel("agent", "My Agent", "drop", "input-topic",
                        "output-topic", Map.of(), null, null)),
                List.of(new ModelBuilder.AgentModel("agent", "My Agent", "drop-fields", "input-topic",
                        "output-topic", Map.of("fields", "f"), null, null)),
                false);

        checkAgents(
                List.of(new ModelBuilder.AgentModel("agent", "My Agent", "drop", "input-topic",
                        "output-topic", Map.of(), null, null)),
                List.of(new ModelBuilder.AgentModel("agent", "My Agent", "drop", "output-topic",
                        "input-topic", Map.of(), null, null)),
                false);

        checkAgents(
                List.of(new ModelBuilder.AgentModel("agent", "My Agent", "drop", "input-topic",
                        "output-topic", Map.of("config1", true), null, null)),
                List.of(new ModelBuilder.AgentModel("agent", "My Agent", "drop", "input-topic",
                        "output-topic", Map.of("config1", false), null, null)),
                true);

        checkAgents(
                List.of(new ModelBuilder.AgentModel("agent", "My Agent", "drop", "input-topic",
                        "output-topic", Map.of("config1", true), null, null)),
                List.of(new ModelBuilder.AgentModel("agent", "My Agent", "drop", "input-topic",
                        "output-topic", Map.of("newConfig", false), null, null)),
                true);

        checkAgents(
                List.of(new ModelBuilder.AgentModel("agent", "My Agent", "drop", "input-topic",
                        "output-topic", Map.of(), new ResourcesSpec(1, 1), null)),
                List.of(new ModelBuilder.AgentModel("agent", "My Agent", "drop", "input-topic",
                        "output-topic", Map.of(), new ResourcesSpec(1, 1), null)),
                true);

        checkAgents(
                List.of(new ModelBuilder.AgentModel("agent", "My Agent", "drop", "input-topic",
                        "output-topic", Map.of(), new ResourcesSpec(1, 1), null)),
                List.of(new ModelBuilder.AgentModel("agent", "My Agent", "drop", "input-topic",
                        "output-topic", Map.of(), new ResourcesSpec(2, 1), null)),
                true);

        checkAgents(
                List.of(new ModelBuilder.AgentModel("agent", "My Agent", "drop", "input-topic",
                        "output-topic", Map.of(), new ResourcesSpec(1, 1), null)),
                List.of(new ModelBuilder.AgentModel("agent", "My Agent", "drop", "input-topic",
                        "output-topic", Map.of(), new ResourcesSpec(1, 2), null)),
                true);

        checkAgents(
                List.of(new ModelBuilder.AgentModel("agent", "My Agent", "drop", "input-topic",
                        "output-topic", Map.of(), new ResourcesSpec(1, 1), null)),
                List.of(new ModelBuilder.AgentModel("agent", "My Agent", "drop", "input-topic",
                        "output-topic", Map.of(), new ResourcesSpec(2, 2), null)),
                true);

        checkAgents(
                List.of(new ModelBuilder.AgentModel("agent", "My Agent", "drop", "input-topic",
                        "output-topic", Map.of(), new ResourcesSpec(2, 2), null)),
                List.of(new ModelBuilder.AgentModel("agent", "My Agent", "drop", "input-topic",
                        "output-topic", Map.of(), new ResourcesSpec(1, 1), null)),
                true);
    }


    private static void checkAgents(List<ModelBuilder.AgentModel> from,
                                    List<ModelBuilder.AgentModel> to, boolean expectValid) {
        final ApplicationService service = getApplicationService();
        try {
            service.validateTopicsUpdate(
                    buildPlanWithModels(from),
                    buildPlanWithModels(to)
            );
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
        @Cleanup ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        final ModelBuilder.PipelineFileModel pipelineFileModel = new ModelBuilder.PipelineFileModel();
        pipelineFileModel.setId("pi");
        pipelineFileModel.setModule("mod");
        pipelineFileModel.setTopics(List.of(
                new ModelBuilder.TopicDefinitionModel("input-topic", null, null, 0, null, null, null),
                new ModelBuilder.TopicDefinitionModel("output-topic", null, null, 0, null, null, null)));
        pipelineFileModel.setPipeline(agents);
        Application applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of("instance.yaml",
                        """
                                instance:
                                  streamingCluster:
                                    type: "noop"     
                                  computeCluster:
                                    type: "none"         
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
                        "module.yaml", SerializationUtil.writeAsYaml(pipelineFileModel)));
        return deployer.createImplementation("app", applicationInstance);
    }

}