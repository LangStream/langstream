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
package ai.langstream.impl.agents.ai;

import ai.langstream.api.doc.AgentConfigurationModel;
import ai.langstream.api.model.AgentConfiguration;
import ai.langstream.api.model.Application;
import ai.langstream.api.model.Module;
import ai.langstream.api.model.Pipeline;
import ai.langstream.api.model.Resource;
import ai.langstream.api.runtime.ComponentType;
import ai.langstream.api.runtime.ComputeClusterRuntime;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.impl.agents.ai.steps.AIChatCompletionsConfiguration;
import ai.langstream.impl.agents.ai.steps.AITextCompletionsConfiguration;
import ai.langstream.impl.agents.ai.steps.CastConfiguration;
import ai.langstream.impl.agents.ai.steps.ComputeAIEmbeddingsConfiguration;
import ai.langstream.impl.agents.ai.steps.ComputeConfiguration;
import ai.langstream.impl.agents.ai.steps.DropConfiguration;
import ai.langstream.impl.agents.ai.steps.DropFieldsConfiguration;
import ai.langstream.impl.agents.ai.steps.FlattenConfiguration;
import ai.langstream.impl.agents.ai.steps.MergeKeyValueConfiguration;
import ai.langstream.impl.agents.ai.steps.QueryConfiguration;
import ai.langstream.impl.agents.ai.steps.UnwrapKeyValueConfiguration;
import ai.langstream.impl.common.AbstractAgentProvider;
import ai.langstream.impl.uti.ClassConfigValidator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GenAIToolKitFunctionAgentProvider extends AbstractAgentProvider {

    private static final Map<String, StepConfigurationInitializer> STEP_TYPES;
    protected static final String SERVICE_VERTEX = "vertex-configuration";
    protected static final String SERVICE_HUGGING_FACE = "hugging-face-configuration";
    protected static final String SERVICE_OPEN_AI = "open-ai-configuration";
    protected static final String SERVICE_BEDROCK = "bedrock-configuration";
    protected static final String SERVICE_OLLAMA = "ollama-configuration";

    protected static final List<String> AI_SERVICES =
            List.of(
                    SERVICE_VERTEX,
                    SERVICE_HUGGING_FACE,
                    SERVICE_OPEN_AI,
                    SERVICE_BEDROCK,
                    SERVICE_OLLAMA);

    static {
        final Map<String, StepConfigurationInitializer> steps = new HashMap<>();
        steps.put("drop-fields", DropFieldsConfiguration.STEP);
        steps.put("merge-key-value", MergeKeyValueConfiguration.STEP);
        steps.put("unwrap-key-value", UnwrapKeyValueConfiguration.STEP);
        steps.put("cast", CastConfiguration.STEP);
        steps.put("flatten", FlattenConfiguration.STEP);
        steps.put("drop", DropConfiguration.STEP);
        steps.put("compute", ComputeConfiguration.STEP);
        steps.put("compute-ai-embeddings", ComputeAIEmbeddingsConfiguration.STEP);
        steps.put("query", QueryConfiguration.STEP);
        steps.put("ai-chat-completions", AIChatCompletionsConfiguration.STEP);
        steps.put("ai-text-completions", AITextCompletionsConfiguration.STEP);
        STEP_TYPES = Collections.unmodifiableMap(steps);
    }

    public GenAIToolKitFunctionAgentProvider(String clusterType) {
        super(STEP_TYPES.keySet(), List.of(clusterType, "none"));
    }

    @Override
    protected final ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        return ComponentType.PROCESSOR;
    }

    @Override
    protected Class getAgentConfigModelClass(String type) {
        StepConfigurationInitializer stepConfigurationInitializer = STEP_TYPES.get(type);
        log.info(
                "Validating agent configuration model for type {} with {}",
                type,
                stepConfigurationInitializer.getAgentConfigurationModelClass());
        return stepConfigurationInitializer.getAgentConfigurationModelClass();
    }

    public interface TopicConfigurationGenerator {
        void generateTopicConfiguration(String topicName);
    }

    public interface StepConfigurationInitializer {
        default Class getAgentConfigurationModelClass() {
            return null;
        }

        default void generateSteps(
                Map<String, Object> step,
                Map<String, Object> originalConfiguration,
                AgentConfiguration agentConfiguration,
                DataSourceConfigurationGenerator dataSourceConfigurationGenerator,
                TopicConfigurationGenerator topicConfigurationGenerator,
                AIServiceConfigurationGenerator aiServiceConfigurationGenerator) {
            final Class modelClazz = getAgentConfigurationModelClass();
            ClassConfigValidator.validateAgentModelFromClass(
                    agentConfiguration, modelClazz, originalConfiguration);
            ClassConfigValidator.generateAgentModelFromClass(modelClazz)
                    .getProperties()
                    .entrySet()
                    .forEach(
                            e -> {
                                Object value = originalConfiguration.get(e.getKey());
                                if (value == null
                                        && !e.getValue().isRequired()
                                        && e.getValue().getDefaultValue() != null) {
                                    value = e.getValue().getDefaultValue();
                                }
                                if (value != null) {
                                    step.put(e.getKey(), value);
                                }
                            });
        }
    }

    protected void generateSteps(
            Module module,
            Map<String, Object> originalConfiguration,
            Map<String, Object> configuration,
            Application application,
            AgentConfiguration agentConfiguration,
            ComputeClusterRuntime computeClusterRuntime,
            PluginsRegistry pluginsRegistry) {
        List<Map<String, Object>> steps = new ArrayList<>();
        configuration.put("steps", steps);
        Map<String, Object> step = new HashMap<>();

        // we are mapping the original name to the ai-tools function name
        step.put("type", agentConfiguration.getType());

        DataSourceConfigurationGenerator dataSourceConfigurationInjector =
                (resourceId) ->
                        generateDataSourceConfiguration(
                                resourceId,
                                application,
                                configuration,
                                computeClusterRuntime,
                                pluginsRegistry);

        TopicConfigurationGenerator topicConfigurationGenerator =
                (topicName) -> {
                    // only resolve the topic definition to verify topic is declared
                    module.resolveTopic(topicName);
                };

        AIServiceConfigurationGenerator aiServiceConfigurationGenerator =
                (resourceId) ->
                        generateAIServiceConfiguration(
                                resourceId,
                                application,
                                configuration,
                                computeClusterRuntime,
                                pluginsRegistry,
                                agentConfiguration);

        STEP_TYPES
                .get(agentConfiguration.getType())
                .generateSteps(
                        step,
                        originalConfiguration,
                        agentConfiguration,
                        dataSourceConfigurationInjector,
                        topicConfigurationGenerator,
                        aiServiceConfigurationGenerator);

        step.remove("composable");

        steps.add(step);
    }

    public interface DataSourceConfigurationGenerator {
        void generateDataSourceConfiguration(String resourceId);
    }

    public interface AIServiceConfigurationGenerator {
        void generateAIServiceConfiguration(String resourceId);
    }

    private void generateAIServiceConfiguration(
            String resourceId,
            Application applicationInstance,
            Map<String, Object> configuration,
            ComputeClusterRuntime clusterRuntime,
            PluginsRegistry pluginsRegistry,
            AgentConfiguration agentConfiguration) {
        if (resourceId != null) {
            Resource resource = applicationInstance.getResources().get(resourceId);
            log.info("Generating ai service configuration for {}", resourceId);
            if (resource != null) {
                final String type = resource.type();
                if (!AI_SERVICES.contains(type)) {
                    throw new IllegalArgumentException(
                            "Resource " + resourceId + " is not in types: " + AI_SERVICES);
                }
                Map<String, Object> resourceImplementation =
                        clusterRuntime.getResourceImplementation(resource, pluginsRegistry);
                final String configKey = getConfigKey(type);
                if (configKey == null) {
                    throw new IllegalArgumentException(
                            "Resource " + resourceId + " is not in types: " + AI_SERVICES);
                }
                configuration.put(configKey, resourceImplementation);
            } else {
                throw new IllegalArgumentException("Resource " + resourceId + " not found");
            }
        } else {
            boolean found = false;
            for (Resource resource : applicationInstance.getResources().values()) {
                final String configKey = getConfigKey(resource.type());
                if (configKey != null) {
                    Map<String, Object> configurationCopy =
                            clusterRuntime.getResourceImplementation(resource, pluginsRegistry);
                    configuration.put(configKey, configurationCopy);
                    found = true;
                }
            }
            if (!found) {
                final String errString =
                        ClassConfigValidator.formatErrString(
                                new ClassConfigValidator.AgentEntityRef(agentConfiguration),
                                "No ai service resource found in application configuration. One of "
                                        + AI_SERVICES.stream().collect(Collectors.joining(", "))
                                        + " must be defined.");
                throw new IllegalArgumentException(errString);
            }
        }
    }

    private static String getConfigKey(String type) {
        return switch (type) {
            case SERVICE_VERTEX -> "vertex";
            case SERVICE_HUGGING_FACE -> "huggingface";
            case SERVICE_OPEN_AI -> "openai";
            case SERVICE_BEDROCK -> "bedrock";
            case SERVICE_OLLAMA -> "ollama";
            default -> null;
        };
    }

    private void generateDataSourceConfiguration(
            String resourceId,
            Application applicationInstance,
            Map<String, Object> configuration,
            ComputeClusterRuntime computeClusterRuntime,
            PluginsRegistry pluginsRegistry) {
        Resource resource = applicationInstance.getResources().get(resourceId);
        log.info("Generating datasource configuration for {}", resourceId);
        if (resource != null) {
            if (!resource.type().equals("datasource")) {
                throw new IllegalArgumentException(
                        "Resource " + resourceId + " is not type=datasource");
            }
            if (configuration.containsKey("datasource")) {
                throw new IllegalArgumentException("Only one datasource is supported");
            }
            Map<String, Object> resourceImplementation =
                    computeClusterRuntime.getResourceImplementation(resource, pluginsRegistry);
            configuration.put("datasource", resourceImplementation);
        } else {
            throw new IllegalArgumentException("Resource " + resourceId + " not found");
        }
    }

    @Override
    protected Map<String, Object> computeAgentConfiguration(
            AgentConfiguration agentConfiguration,
            Module module,
            Pipeline pipeline,
            ExecutionPlan executionPlan,
            ComputeClusterRuntime clusterRuntime,
            PluginsRegistry pluginsRegistry) {
        Map<String, Object> originalConfiguration =
                super.computeAgentConfiguration(
                        agentConfiguration,
                        module,
                        pipeline,
                        executionPlan,
                        clusterRuntime,
                        pluginsRegistry);
        Map<String, Object> configuration = new HashMap<>();

        generateSteps(
                module,
                originalConfiguration,
                configuration,
                executionPlan.getApplication(),
                agentConfiguration,
                clusterRuntime,
                pluginsRegistry);
        return configuration;
    }

    protected static <T> T requiredField(
            Map<String, Object> step,
            AgentConfiguration agentConfiguration,
            Map<String, Object> newConfiguration,
            String name) {
        if (!newConfiguration.containsKey(name)) {
            throw new IllegalArgumentException(
                    "Missing required field '"
                            + name
                            + "' in agent definition, type="
                            + agentConfiguration.getType()
                            + ", name="
                            + agentConfiguration.getName()
                            + ", id="
                            + agentConfiguration.getId());
        }
        Object value = newConfiguration.get(name);
        step.put(name, value);
        return (T) value;
    }

    protected static <T> T optionalField(
            Map<String, Object> step,
            AgentConfiguration agentConfiguration,
            Map<String, Object> newConfiguration,
            String name,
            Object defaultValue) {
        if (!newConfiguration.containsKey(name)) {
            if (defaultValue != null) {
                step.put(name, defaultValue);
            }
            return (T) defaultValue;
        } else {
            step.put(name, newConfiguration.get(name));
            return (T) newConfiguration.get(name);
        }
    }

    @Override
    public Map<String, AgentConfigurationModel> generateSupportedTypesDocumentation() {
        Map<String, AgentConfigurationModel> result = new LinkedHashMap<>();
        STEP_TYPES.forEach(
                (type, stepType) -> {
                    final Class modelClass = stepType.getAgentConfigurationModelClass();
                    result.put(
                            type,
                            modelClass == null
                                    ? new AgentConfigurationModel()
                                    : ClassConfigValidator.generateAgentModelFromClass(modelClass));
                });
        return result;
    }
}
