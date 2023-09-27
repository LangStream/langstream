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

import ai.langstream.api.model.AgentConfiguration;
import ai.langstream.api.model.Application;
import ai.langstream.api.model.Module;
import ai.langstream.api.model.Pipeline;
import ai.langstream.api.model.Resource;
import ai.langstream.api.model.TopicDefinition;
import ai.langstream.api.runtime.ComponentType;
import ai.langstream.api.runtime.ComputeClusterRuntime;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.impl.common.AbstractAgentProvider;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GenAIToolKitFunctionAgentProvider extends AbstractAgentProvider {

    protected static final StepConfigurationInitializer DROP_FIELDS =
            new StepConfigurationInitializer() {
                @Override
                public void generateSteps(
                        Map<String, Object> step,
                        Map<String, Object> originalConfiguration,
                        AgentConfiguration agentConfiguration,
                        DataSourceConfigurationGenerator dataSourceConfigurationGenerator,
                        TopicConfigurationGenerator topicConfigurationGenerator) {
                    requiredField(step, agentConfiguration, originalConfiguration, "fields");
                    optionalField(step, agentConfiguration, originalConfiguration, "part", null);
                }
            };
    protected static final StepConfigurationInitializer UNWRAP_KEY_VALUE =
            new StepConfigurationInitializer() {
                @Override
                public void generateSteps(
                        Map<String, Object> step,
                        Map<String, Object> originalConfiguration,
                        AgentConfiguration agentConfiguration,
                        DataSourceConfigurationGenerator dataSourceConfigurationGenerator,
                        TopicConfigurationGenerator topicConfigurationGenerator) {
                    optionalField(
                            step, agentConfiguration, originalConfiguration, "unwrapKey", null);
                }
            };
    protected static final StepConfigurationInitializer CAST =
            new StepConfigurationInitializer() {
                @Override
                public void generateSteps(
                        Map<String, Object> step,
                        Map<String, Object> originalConfiguration,
                        AgentConfiguration agentConfiguration,
                        DataSourceConfigurationGenerator dataSourceConfigurationGenerator,
                        TopicConfigurationGenerator topicConfigurationGenerator) {
                    requiredField(step, agentConfiguration, originalConfiguration, "schema-type");
                    optionalField(step, agentConfiguration, originalConfiguration, "part", null);
                }
            };
    protected static final StepConfigurationInitializer FLATTEN =
            new StepConfigurationInitializer() {
                @Override
                public void generateSteps(
                        Map<String, Object> step,
                        Map<String, Object> originalConfiguration,
                        AgentConfiguration agentConfiguration,
                        DataSourceConfigurationGenerator dataSourceConfigurationGenerator,
                        TopicConfigurationGenerator topicConfigurationGenerator) {
                    optionalField(
                            step, agentConfiguration, originalConfiguration, "delimiter", null);
                    optionalField(step, agentConfiguration, originalConfiguration, "part", null);
                }
            };
    protected static final StepConfigurationInitializer COMPUTE =
            new StepConfigurationInitializer() {
                @Override
                public void generateSteps(
                        Map<String, Object> step,
                        Map<String, Object> originalConfiguration,
                        AgentConfiguration agentConfiguration,
                        DataSourceConfigurationGenerator dataSourceConfigurationGenerator,
                        TopicConfigurationGenerator topicConfigurationGenerator) {
                    requiredField(step, agentConfiguration, originalConfiguration, "fields");
                }
            };
    protected static final StepConfigurationInitializer COMPUTE_AI_EMBEDDINGS =
            new StepConfigurationInitializer() {
                @Override
                public void generateSteps(
                        Map<String, Object> step,
                        Map<String, Object> originalConfiguration,
                        AgentConfiguration agentConfiguration,
                        DataSourceConfigurationGenerator dataSourceConfigurationGenerator,
                        TopicConfigurationGenerator topicConfigurationGenerator) {
                    optionalField(
                            step,
                            agentConfiguration,
                            originalConfiguration,
                            "model",
                            "text-embedding-ada-002");
                    optionalField(
                            step, agentConfiguration, originalConfiguration, "batch-size", null);
                    optionalField(
                            step, agentConfiguration, originalConfiguration, "concurrency", null);
                    optionalField(
                            step,
                            agentConfiguration,
                            originalConfiguration,
                            "flush-interval",
                            null);
                    requiredField(
                            step, agentConfiguration, originalConfiguration, "embeddings-field");
                    requiredField(step, agentConfiguration, originalConfiguration, "text");
                }
            };
    protected static final StepConfigurationInitializer QUERY =
            new StepConfigurationInitializer() {
                @Override
                public void generateSteps(
                        Map<String, Object> step,
                        Map<String, Object> originalConfiguration,
                        AgentConfiguration agentConfiguration,
                        DataSourceConfigurationGenerator dataSourceConfigurationGenerator,
                        TopicConfigurationGenerator topicConfigurationGenerator) {

                    // reference to datasource
                    String datasource = (String) originalConfiguration.remove("datasource");
                    if (datasource == null) {
                        // error
                        requiredField(
                                step, agentConfiguration, originalConfiguration, "datasource");
                        return;
                    }
                    dataSourceConfigurationGenerator.generateDataSourceConfiguration(datasource);

                    requiredField(step, agentConfiguration, originalConfiguration, "fields");
                    requiredField(step, agentConfiguration, originalConfiguration, "query");
                    requiredField(step, agentConfiguration, originalConfiguration, "output-field");
                    optionalField(
                            step, agentConfiguration, originalConfiguration, "only-first", null);
                }
            };
    protected static final StepConfigurationInitializer AI_CHAT_COMPLETIONS =
            new StepConfigurationInitializer() {
                @Override
                public void generateSteps(
                        Map<String, Object> step,
                        Map<String, Object> newConfiguration,
                        AgentConfiguration agentConfiguration,
                        DataSourceConfigurationGenerator dataSourceConfigurationGenerator,
                        TopicConfigurationGenerator topicConfigurationGenerator) {
                    requiredField(step, agentConfiguration, newConfiguration, "completion-field");
                    optionalField(step, agentConfiguration, newConfiguration, "log-field", null);
                    optionalField(
                            step,
                            agentConfiguration,
                            newConfiguration,
                            "min-chunks-per-message",
                            null);
                    optionalField(
                            step,
                            agentConfiguration,
                            newConfiguration,
                            "stream-response-completion-field",
                            null);
                    String streamTopic =
                            optionalField(
                                    step,
                                    agentConfiguration,
                                    newConfiguration,
                                    "stream-to-topic",
                                    null);
                    if (streamTopic != null) {
                        Map<String, Object> topicConfiguration =
                                topicConfigurationGenerator.generateTopicConfiguration(streamTopic);
                        newConfiguration.put("streamTopicConfiguration", topicConfiguration);
                    }
                    Object messages =
                            requiredField(step, agentConfiguration, newConfiguration, "messages");
                    if (messages instanceof Collection<?> collection) {
                        for (Object o : collection) {
                            if (o instanceof Map map) {
                                map.keySet()
                                        .forEach(
                                                k -> {
                                                    if (!"role".equals(k) && !"content".equals(k)) {
                                                        throw new IllegalArgumentException(
                                                                "messages must be a list of objects, [{role: 'user', "
                                                                        + "content: 'template'}]");
                                                    }
                                                });
                            } else {
                                throw new IllegalArgumentException(
                                        "messages must be a list of objects, [{role: 'user', content: 'template'}]");
                            }
                        }
                    } else {
                        throw new IllegalArgumentException(
                                "messages must be a list of objects: [{role: 'user', content: 'template'}]");
                    }
                    requiredField(step, agentConfiguration, newConfiguration, "model");
                    optionalField(step, agentConfiguration, newConfiguration, "temperature", null);
                    optionalField(step, agentConfiguration, newConfiguration, "top-p", null);
                    optionalField(step, agentConfiguration, newConfiguration, "logit-bias", null);
                    optionalField(step, agentConfiguration, newConfiguration, "stop", null);
                    optionalField(step, agentConfiguration, newConfiguration, "max-tokens", null);
                    optionalField(
                            step, agentConfiguration, newConfiguration, "presence-penalty", null);
                    optionalField(
                            step, agentConfiguration, newConfiguration, "frequency-penalty", null);
                    optionalField(step, agentConfiguration, newConfiguration, "user", null);
                }
            };
    protected static final StepConfigurationInitializer AI_TEXT_COMPLETIONS =
            new StepConfigurationInitializer() {
                @Override
                public void generateSteps(
                        Map<String, Object> step,
                        Map<String, Object> newConfiguration,
                        AgentConfiguration agentConfiguration,
                        DataSourceConfigurationGenerator dataSourceConfigurationGenerator,
                        TopicConfigurationGenerator topicConfigurationGenerator) {
                    requiredField(step, agentConfiguration, newConfiguration, "completion-field");
                    optionalField(step, agentConfiguration, newConfiguration, "log-field", null);
                    optionalField(
                            step,
                            agentConfiguration,
                            newConfiguration,
                            "min-chunks-per-message",
                            null);
                    optionalField(
                            step,
                            agentConfiguration,
                            newConfiguration,
                            "stream-response-completion-field",
                            null);
                    String streamTopic =
                            optionalField(
                                    step,
                                    agentConfiguration,
                                    newConfiguration,
                                    "stream-to-topic",
                                    null);
                    if (streamTopic != null) {
                        Map<String, Object> topicConfiguration =
                                topicConfigurationGenerator.generateTopicConfiguration(streamTopic);
                        newConfiguration.put("streamTopicConfiguration", topicConfiguration);
                    }
                    requiredField(step, agentConfiguration, newConfiguration, "prompt");
                    requiredField(step, agentConfiguration, newConfiguration, "model");
                    optionalField(step, agentConfiguration, newConfiguration, "temperature", null);
                    optionalField(step, agentConfiguration, newConfiguration, "top-p", null);
                    optionalField(step, agentConfiguration, newConfiguration, "logit-bias", null);
                    optionalField(step, agentConfiguration, newConfiguration, "stop", null);
                    optionalField(step, agentConfiguration, newConfiguration, "max-tokens", null);
                    optionalField(
                            step, agentConfiguration, newConfiguration, "presence-penalty", null);
                    optionalField(
                            step, agentConfiguration, newConfiguration, "frequency-penalty", null);
                    optionalField(step, agentConfiguration, newConfiguration, "user", null);
                }
            };
    private static final Map<String, StepConfigurationInitializer> STEP_TYPES;

    static {
        final Map<String, StepConfigurationInitializer> steps = new HashMap<>();
        steps.put("drop-fields", DROP_FIELDS);
        steps.put("merge-key-value", new StepConfigurationInitializer() {});
        steps.put("unwrap-key-value", UNWRAP_KEY_VALUE);
        steps.put("cast", CAST);
        steps.put("flatten", FLATTEN);
        steps.put("drop", new StepConfigurationInitializer() {});
        steps.put("compute", COMPUTE);
        steps.put("compute-ai-embeddings", COMPUTE_AI_EMBEDDINGS);
        steps.put("query", QUERY);
        steps.put("ai-chat-completions", AI_CHAT_COMPLETIONS);
        steps.put("ai-text-completions", AI_TEXT_COMPLETIONS);
        STEP_TYPES = Collections.unmodifiableMap(steps);
    }

    public GenAIToolKitFunctionAgentProvider(String clusterType) {
        super(STEP_TYPES.keySet(), List.of(clusterType, "none"));
    }

    @Override
    protected final ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        return ComponentType.PROCESSOR;
    }

    interface TopicConfigurationGenerator {
        Map<String, Object> generateTopicConfiguration(String topicName);
    }

    private interface StepConfigurationInitializer {
        default void generateSteps(
                Map<String, Object> step,
                Map<String, Object> originalConfiguration,
                AgentConfiguration agentConfiguration,
                DataSourceConfigurationGenerator dataSourceConfigurationGenerator,
                TopicConfigurationGenerator topicConfigurationGenerator) {}
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

        // on every step you can put a "when" clause
        optionalField(step, agentConfiguration, originalConfiguration, "when", null);

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
                    TopicDefinition topicDefinition = module.resolveTopic(topicName);
                    return topicDefinition.getConfig();
                };

        STEP_TYPES
                .get(agentConfiguration.getType())
                .generateSteps(
                        step,
                        originalConfiguration,
                        agentConfiguration,
                        dataSourceConfigurationInjector,
                        topicConfigurationGenerator);
        steps.add(step);
    }

    interface DataSourceConfigurationGenerator {
        void generateDataSourceConfiguration(String resourceId);
    }

    private void generateAIProvidersConfiguration(
            Application applicationInstance,
            Map<String, Object> originalConfiguration,
            Map<String, Object> configuration,
            ComputeClusterRuntime clusterRuntime,
            PluginsRegistry pluginsRegistry) {
        // let the user force the provider or detect it automatically
        String service = (String) originalConfiguration.get("service");
        for (Resource resource : applicationInstance.getResources().values()) {
            Map<String, Object> configurationCopy =
                    clusterRuntime.getResourceImplementation(resource, pluginsRegistry);
            switch (resource.type()) {
                case "vertex-configuration":
                    if (service == null || service.equals("vertex")) {
                        configuration.put("vertex", configurationCopy);
                    }
                    break;
                case "hugging-face-configuration":
                    if (service == null || service.equals("hugging-face")) {
                        configuration.put("huggingface", configurationCopy);
                    }
                    break;
                case "open-ai-configuration":
                    if (service == null || service.equals("open-ai")) {
                        configuration.put("openai", configurationCopy);
                    }
                    break;
                default:
                    // ignore
            }
        }
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

        generateAIProvidersConfiguration(
                executionPlan.getApplication(),
                originalConfiguration,
                configuration, clusterRuntime, pluginsRegistry);

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
}
