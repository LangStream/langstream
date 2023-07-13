package com.datastax.oss.sga.impl.agents.ai;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.Resource;
import com.datastax.oss.sga.api.runtime.AgentNode;
import com.datastax.oss.sga.api.runtime.ComponentType;
import com.datastax.oss.sga.api.runtime.ComputeClusterRuntime;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.impl.common.AbstractAgentProvider;
import com.datastax.oss.sga.impl.common.DefaultAgentNode;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

@Slf4j
public class GenAIToolKitFunctionAgentProvider extends AbstractAgentProvider {

    private static final Map<String, StepConfigurationInitializer> STEP_TYPES = Map.of(

            "drop-fields", new StepConfigurationInitializer() {
                @Override
                public void generateSteps(Map<String, Object> step, Map<String, Object> originalConfiguration, AgentConfiguration agentConfiguration,
                                          DataSourceConfigurationGenerator dataSourceConfigurationGenerator) {
                    requiredField(step, agentConfiguration, originalConfiguration, "fields");
                    optionalField(step, agentConfiguration, originalConfiguration, "part", null);
                }
            },
            "merge-key-value", new StepConfigurationInitializer() {
            },
            "unwrap-key-value", new StepConfigurationInitializer() {
                @Override
                public void generateSteps(Map<String, Object> step, Map<String, Object> originalConfiguration, AgentConfiguration agentConfiguration,
                                          DataSourceConfigurationGenerator dataSourceConfigurationGenerator) {
                    optionalField(step, agentConfiguration, originalConfiguration, "unwrapKey", null);
                }
            },
            "cast", new StepConfigurationInitializer() {
                @Override
                public void generateSteps(Map<String, Object> step, Map<String, Object> originalConfiguration, AgentConfiguration agentConfiguration,
                                          DataSourceConfigurationGenerator dataSourceConfigurationGenerator) {
                    requiredField(step, agentConfiguration, originalConfiguration, "schema-type");
                    optionalField(step, agentConfiguration, originalConfiguration, "part", null);
                }
            },
            "flatten", new StepConfigurationInitializer() {
                @Override
                public void generateSteps(Map<String, Object> step, Map<String, Object> originalConfiguration, AgentConfiguration agentConfiguration,
                                          DataSourceConfigurationGenerator dataSourceConfigurationGenerator) {
                    optionalField(step, agentConfiguration, originalConfiguration, "delimiter", null);
                    optionalField(step, agentConfiguration, originalConfiguration, "part", null);
                }
            },
            "drop", new StepConfigurationInitializer() {
            },
            "compute", new StepConfigurationInitializer() {
                @Override
                public void generateSteps(Map<String, Object> step, Map<String, Object> originalConfiguration,
                                          AgentConfiguration agentConfiguration, DataSourceConfigurationGenerator dataSourceConfigurationGenerator) {
                    requiredField(step, agentConfiguration, originalConfiguration, "fields");
                }
            },
            "compute-ai-embeddings", new StepConfigurationInitializer() {
                @Override
                public void generateSteps(Map<String, Object> step, Map<String, Object> originalConfiguration, AgentConfiguration agentConfiguration,
                                          DataSourceConfigurationGenerator dataSourceConfigurationGenerator) {
                    optionalField(step, agentConfiguration, originalConfiguration, "model", "text-embedding-ada-002");
                    requiredField(step, agentConfiguration, originalConfiguration, "embeddings-field");
                    requiredField(step, agentConfiguration, originalConfiguration, "text");
                }
            },
            "query", new StepConfigurationInitializer() {
                @Override
                public void generateSteps(Map<String, Object> step, Map<String, Object> originalConfiguration, AgentConfiguration agentConfiguration,
                                          DataSourceConfigurationGenerator dataSourceConfigurationGenerator) {

                    // reference to datasource
                    String datasource = (String) originalConfiguration.remove("datasource");
                    if (datasource == null) {
                        // error
                        requiredField(step, agentConfiguration, originalConfiguration, "datasource");
                        return;
                    }
                    dataSourceConfigurationGenerator.generateDataSourceConfiguration(datasource);

                    requiredField(step, agentConfiguration, originalConfiguration, "fields");
                    requiredField(step, agentConfiguration, originalConfiguration, "query");
                    requiredField(step, agentConfiguration, originalConfiguration, "output-field");
                }
            },
            "chat-ai-completions", new StepConfigurationInitializer() {
                @Override
                public void generateSteps(Map<String, Object> step, Map<String, Object> originalConfiguration, AgentConfiguration agentConfiguration,
                                          DataSourceConfigurationGenerator dataSourceConfigurationGenerator) {
                    requiredField(step, agentConfiguration, originalConfiguration, "output-field");
                    requiredField(step, agentConfiguration, originalConfiguration, "messages");
                    requiredField(step, agentConfiguration, originalConfiguration, "model");
                    optionalField(step, agentConfiguration, originalConfiguration, "temperature", null);
                    optionalField(step, agentConfiguration, originalConfiguration, "top-p", null);
                    optionalField(step, agentConfiguration, originalConfiguration, "stop", null);
                    optionalField(step, agentConfiguration, originalConfiguration, "max-tokens", null);
                    optionalField(step, agentConfiguration, originalConfiguration, "presence-penalty", null);
                    optionalField(step, agentConfiguration, originalConfiguration, "frequency-penalty", null);
                    optionalField(step, agentConfiguration, originalConfiguration, "user", null);
                }
            }

    );
    private final String mainAgentType;
    public GenAIToolKitFunctionAgentProvider(String clusterType, String mainAgentType) {
        super(STEP_TYPES.keySet(), List.of(clusterType));
        this.mainAgentType = mainAgentType;
    }

    @Override
    protected final ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        return ComponentType.FUNCTION;
    }

    @Override
    protected String getAgentType(AgentConfiguration agentConfiguration) {
        // all the agents in the AT ToolKit can be generated from one single agent implementation
        // this is important because the runtime is able to "merge" agents of the same type
        return mainAgentType;
    }

    private interface StepConfigurationInitializer {
        default void generateSteps(Map<String, Object> step,
                           Map<String, Object> originalConfiguration,
                           AgentConfiguration agentConfiguration,
                           DataSourceConfigurationGenerator dataSourceConfigurationGenerator) {}
    }

    protected void generateSteps(Map<String, Object> originalConfiguration,
                                 Map<String, Object> configuration,
                                 Application application,
                                 AgentConfiguration agentConfiguration) {
        List<Map<String, Object>> steps = new ArrayList<>();
        configuration.put("steps", steps);
        Map<String, Object> step = new HashMap<>();

        // we are mapping the original name to the ai-tools function name
        step.put("type", agentConfiguration.getType());

        // on every step you can put a "when" clause
        optionalField(step, agentConfiguration, originalConfiguration, "when", null);

        DataSourceConfigurationGenerator dataSourceConfigurationInjector = (resourceId) -> {
            generateDataSourceConfiguration(resourceId, application, configuration);
        };

        STEP_TYPES.get(agentConfiguration.getType())
                .generateSteps(step, originalConfiguration, agentConfiguration, dataSourceConfigurationInjector);
        steps.add(step);
    }

    interface  DataSourceConfigurationGenerator {
        void generateDataSourceConfiguration(String resourceId);
    }


    private void generateOpenAIConfiguration(Application applicationInstance, Map<String, Object> configuration) {
        Resource resource = applicationInstance.getResources().values().stream()
                .filter(r -> r.type().equals("open-ai-configuration"))
                .findFirst().orElse(null);
        if (resource != null) {
            Map<String, Object> openaiConfiguration = new HashMap<>(resource.configuration());
            configuration.put("openai", openaiConfiguration);
        }
    }

    private void generateHuggingFaceConfiguration(Application applicationInstance, Map<String, Object> configuration) {
        Resource resource = applicationInstance.getResources().values().stream()
                .filter(r -> r.type().equals("hugging-face-configuration"))
                .findFirst().orElse(null);
        if (resource != null) {
            Map<String, Object> huggingfaceConfiguration = new HashMap<>(resource.configuration());
            configuration.put("huggingface", huggingfaceConfiguration);
        }
    }

    private void generateDataSourceConfiguration(String resourceId, Application applicationInstance, Map<String, Object> configuration) {
        Resource resource = applicationInstance.getResources().get(resourceId);
        log.info("Generating datasource configuration for {}", resourceId);
        if (resource != null) {
            if (!resource.type().equals("datasource")) {
                throw new IllegalArgumentException("Resource " + resourceId + " is not type=datasource");
            }
            if (configuration.containsKey("datasource")) {
                throw new IllegalArgumentException("Only one datasource is supported");
            }
            configuration.put("datasource", new HashMap<>(resource.configuration()));
        } else {
            throw new IllegalArgumentException("Resource " + resourceId + " not found");
        }
    }

    @Override
    protected Map<String, Object> computeAgentConfiguration(AgentConfiguration agentConfiguration, Module module,
                                                            ExecutionPlan executionPlan,
                                                            ComputeClusterRuntime clusterRuntime) {
        Map<String, Object> originalConfiguration = super.computeAgentConfiguration(agentConfiguration, module, executionPlan, clusterRuntime);
        Map<String, Object> configuration = new HashMap<>();

        generateOpenAIConfiguration(executionPlan.getApplication(), configuration);
        generateHuggingFaceConfiguration(executionPlan.getApplication(), configuration);

        generateSteps(originalConfiguration, configuration, executionPlan.getApplication(), agentConfiguration);
        return configuration;
    }


    @Override
    public boolean canMerge(AgentNode previousAgent, AgentNode agentImplementation) {
        if (Objects.equals(previousAgent.getAgentType(), agentImplementation.getAgentType())
            && previousAgent instanceof DefaultAgentNode agent1
            && agentImplementation instanceof DefaultAgentNode agent2) {
                Map<String, Object> purgedConfiguration1 = new HashMap<>(agent1.getConfiguration());
                purgedConfiguration1.remove("steps");
                Map<String, Object> purgedConfiguration2 = new HashMap<>(agent2.getConfiguration());
                purgedConfiguration2.remove("steps");

                // test query steps with different datasources
                Object datasource1 = purgedConfiguration1.remove("datasource");
                Object datasource2 = purgedConfiguration2.remove("datasource");

                if (datasource1 != null && datasource2 != null) {
                    log.info("Comparing datasources {} and {}", datasource1, datasource2);
                     if (!Objects.equals(datasource1, datasource2)) {
                         log.info("Agents {} and {} cannot be merged (different datasources)",
                                 previousAgent, agentImplementation);
                         return false;
                     }
                }

                log.info("Comparing {} and {}", purgedConfiguration1, purgedConfiguration2);
                return purgedConfiguration1.equals(purgedConfiguration2);
        }
        log.info("Agents {} and {} cannot be merged", previousAgent, agentImplementation);
        return false;
    }

    @Override
    public AgentNode mergeAgents(AgentNode previousAgent, AgentNode agentImplementation,
                                 ExecutionPlan applicationInstance) {
        if (Objects.equals(previousAgent.getAgentType(), agentImplementation.getAgentType())
                && previousAgent instanceof DefaultAgentNode agent1
                && agentImplementation instanceof DefaultAgentNode agent2) {

            log.info("Merging agents");
            log.info("Agent 1: {}", agent1);
            log.info("Agent 2: {}", agent2);

            Map<String, Object> configurationWithoutSteps1 = new HashMap<>(agent1.getConfiguration());
            List<Map<String, Object>> steps1 = (List<Map<String, Object>>) configurationWithoutSteps1.remove("steps");
            Map<String, Object> configurationWithoutSteps2 = new HashMap<>(agent2.getConfiguration());
            List<Map<String, Object>> steps2 = (List<Map<String, Object>>) configurationWithoutSteps2.remove("steps");

            List<Map<String, Object>> mergedSteps = new ArrayList<>();
            mergedSteps.addAll(steps1);
            mergedSteps.addAll(steps2);

            Map<String, Object> result = new HashMap<>();
            result.putAll(configurationWithoutSteps1);
            result.put("steps", mergedSteps);

            agent1.overrideConfigurationAfterMerge(result, agent2.getOutputConnection());
            log.info("Agent 1 modified: {}", agent1);

            log.info("Discarding topic {}", agent2.getInputConnection());
            applicationInstance.discardTopic(agent2.getInputConnection());
            return previousAgent;
        }
        throw new IllegalStateException();
    }

    protected static void requiredField(Map<String, Object> step, AgentConfiguration agentConfiguration, Map<String, Object> originalConfiguration, String name) {
        if (!originalConfiguration.containsKey(name)) {
            throw new IllegalArgumentException("Missing required field " + name + " in agent definition, type=" + agentConfiguration.getType()
                    + ", name="+agentConfiguration.getName());
        }
        step.put(name,originalConfiguration.get(name));
    }

    protected static void optionalField(Map<String, Object> step, AgentConfiguration agentConfiguration, Map<String, Object> originalConfiguration, String name, Object defaultValue) {
        if (!originalConfiguration.containsKey(name)) {
            if (defaultValue != null) {
                step.put(name, defaultValue);
            }
        } else {
            step.put(name,originalConfiguration.get(name));
        }
    }
}
