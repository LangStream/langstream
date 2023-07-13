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

@Slf4j
public class GenAIToolKitFunctionAgentProvider extends AbstractAgentProvider {

    private static final Map<String, StepConfigurationInitializer> STEP_TYPES = Map.of(
            "compute-ai-embeddings", new StepConfigurationInitializer() {
                @Override
                public void generateSteps(Map<String, Object> step, Map<String, Object> originalConfiguration, AgentConfiguration agentConfiguration) {
                    optionalField(step, agentConfiguration, originalConfiguration, "model", "text-embedding-ada-002");
                    requiredField(step, agentConfiguration, originalConfiguration, "embeddings-field");
                    requiredField(step, agentConfiguration, originalConfiguration, "text");
                }
            },
            "drop-fields", new StepConfigurationInitializer() {
                @Override
                public void generateSteps(Map<String, Object> step, Map<String, Object> originalConfiguration, AgentConfiguration agentConfiguration) {
                    requiredField(step, agentConfiguration, originalConfiguration, "fields");
                    optionalField(step, agentConfiguration, originalConfiguration, "part", null);
                }
            },
            "merge-key-value", new StepConfigurationInitializer() {
            },
            "unwrap-key-value", new StepConfigurationInitializer() {
            },
            "cast", new StepConfigurationInitializer() {
                @Override
                public void generateSteps(Map<String, Object> step, Map<String, Object> originalConfiguration, AgentConfiguration agentConfiguration) {
                    requiredField(step, agentConfiguration, originalConfiguration, "schema-type");
                    optionalField(step, agentConfiguration, originalConfiguration, "part", null);
                }},
                "flatten", new StepConfigurationInitializer() {
                    @Override
                    public void generateSteps(Map<String, Object> step, Map<String, Object> originalConfiguration, AgentConfiguration agentConfiguration)
                    {
                        optionalField(step, agentConfiguration, originalConfiguration, "delimiter", null);
                        optionalField(step, agentConfiguration, originalConfiguration, "part", null);
                    }},
            "drop", new StepConfigurationInitializer() {},
            "compute", new StepConfigurationInitializer() {
                @Override
                public void generateSteps(Map<String, Object> step, Map<String, Object> originalConfiguration, AgentConfiguration agentConfiguration) {
                    requiredField(step, agentConfiguration, originalConfiguration, "fields");
                }}

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
                           AgentConfiguration agentConfiguration) {}
    }

    protected void generateSteps(Map<String, Object> originalConfiguration, List<Map<String, Object>> steps, AgentConfiguration agentConfiguration) {
        Map<String, Object> step = new HashMap<>();

        // we are mapping the original name to the ai-tools function name
        step.put("type", agentConfiguration.getType());

        // on every step you can put a "when" clause
        optionalField(step, agentConfiguration, originalConfiguration, "when", null);

        STEP_TYPES.get(agentConfiguration.getType())
                .generateSteps(step, originalConfiguration, agentConfiguration);
    }

    private void generateOpenAIConfiguration(Application applicationInstance, Map<String, Object> configuration) {
        Resource resource = applicationInstance.getResources().values().stream()
                .filter(r -> r.type().equals("open-ai-configuration"))
                .findFirst().orElse(null);
        if (resource != null) {
            String url = (String) resource.configuration().get("url");
            String accessKey = (String) resource.configuration().get("access-key");
            String provider = (String) resource.configuration().get("provider");
            Map<String, Object> openaiConfiguration = new HashMap<>();
            if (url != null) {
                openaiConfiguration.put("url", url);
            }
            if (accessKey != null) {
                openaiConfiguration.put("access-key", accessKey);
            }
            if (provider != null) {
                openaiConfiguration.put("provider", provider);
            }
            configuration.put("openai", openaiConfiguration);
        }
    }

    @Override
    protected Map<String, Object> computeAgentConfiguration(AgentConfiguration agentConfiguration, Module module,
                                                            ExecutionPlan physicalApplicationInstance,
                                                            ComputeClusterRuntime clusterRuntime) {
        Map<String, Object> originalConfiguration = super.computeAgentConfiguration(agentConfiguration, module, physicalApplicationInstance, clusterRuntime);
        Map<String, Object> configuration = new HashMap<>();

        generateOpenAIConfiguration(physicalApplicationInstance.getApplication(), configuration);

        List<Map<String, Object>> steps = new ArrayList<>();
        configuration.put("steps", steps);
        generateSteps(originalConfiguration, steps, agentConfiguration);
        return configuration;
    }


    @Override
    public boolean canMerge(AgentNode previousAgent, AgentNode agentImplementation) {
        if (Objects.equals(previousAgent.getAgentType(), agentImplementation.getAgentType())
            && previousAgent instanceof DefaultAgentNode agent1
            && agentImplementation instanceof DefaultAgentNode agent2) {
                Map<String, Object> configurationWithoutSteps1 = new HashMap<>(agent1.getConfiguration());
                configurationWithoutSteps1.remove("steps");
                Map<String, Object> configurationWithoutSteps2 = new HashMap<>(agent2.getConfiguration());
                configurationWithoutSteps2.remove("steps");
                log.info("Comparing {} and {}", configurationWithoutSteps1, configurationWithoutSteps2);
                return configurationWithoutSteps1.equals(configurationWithoutSteps2);
            }
        return false;
    }

    @Override
    public AgentNode mergeAgents(AgentNode previousAgent, AgentNode agentImplementation,
                                 ExecutionPlan applicationInstance) {
        if (Objects.equals(previousAgent.getAgentType(), agentImplementation.getAgentType())
                && previousAgent instanceof DefaultAgentNode agent1
                && agentImplementation instanceof DefaultAgentNode agent2) {
            Map<String, Object> configurationWithoutSteps1 = new HashMap<>(agent1.getConfiguration());
            List<Map<String, Object>> steps1 = (List<Map<String, Object>>) configurationWithoutSteps1.remove("steps");
            Map<String, Object> configurationWithoutSteps2 = new HashMap<>(agent2.getConfiguration());
            List<Map<String, Object>> steps2 = (List<Map<String, Object>>) configurationWithoutSteps2.remove("steps");

            List<Map<String, Object>> mergedSteps = new ArrayList<>();
            mergedSteps.addAll(steps2);
            mergedSteps.addAll(steps1);

            Map<String, Object> result = new HashMap<>();
            result.putAll(configurationWithoutSteps1);
            result.put("steps", mergedSteps);

            agent1.overrideConfigurationAfterMerge(result, agent2.getOutputConnection());

            log.info("Discarding topic {}", agent1.getInputConnection());
            applicationInstance.discardTopic(agent1.getInputConnection());
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
