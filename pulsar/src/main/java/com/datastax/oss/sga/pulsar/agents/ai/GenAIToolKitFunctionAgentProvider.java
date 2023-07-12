package com.datastax.oss.sga.pulsar.agents.ai;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.Resource;
import com.datastax.oss.sga.api.runtime.AgentNode;
import com.datastax.oss.sga.api.runtime.ComputeClusterRuntime;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.impl.common.DefaultAgent;
import com.datastax.oss.sga.pulsar.PulsarClusterRuntime;
import com.datastax.oss.sga.pulsar.agents.AbstractPulsarAgentProvider;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Slf4j
public class GenAIToolKitFunctionAgentProvider extends AbstractPulsarAgentProvider {

    private static final String FUNCTION_TYPE = "ai-tools";

    public GenAIToolKitFunctionAgentProvider(String stepType) {
        super(List.of(stepType), List.of(PulsarClusterRuntime.CLUSTER_TYPE));
    }

    @Override
    protected String getAgentType(AgentConfiguration agentConfiguration) {
        // https://github.com/datastax/pulsar-transformations/tree/master/pulsar-ai-tools
        return FUNCTION_TYPE;
    }

    @Override
    protected ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        return ComponentType.FUNCTION;
    }

    protected void generateSteps(Map<String, Object> originalConfiguration, List<Map<String, Object>> steps) {
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
        generateSteps(originalConfiguration, steps);
        return configuration;
    }


    @Override
    public boolean canMerge(AgentNode previousAgent, AgentNode agentImplementation) {
        if (previousAgent instanceof DefaultAgent agent1
                && agent1.getCustomMetadata() instanceof PulsarAgentNodeMetadata metadata1
                && agentImplementation instanceof DefaultAgent agent2
                && agent2.getCustomMetadata() instanceof PulsarAgentNodeMetadata metadata2)
            if (Objects.equals(metadata1.getAgentType(), FUNCTION_TYPE)
                    && Objects.equals(metadata2.getAgentType(), FUNCTION_TYPE)) {
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
        if (previousAgent instanceof DefaultAgent agent1
                && agent1.getCustomMetadata() instanceof PulsarAgentNodeMetadata metadata1
                && agentImplementation instanceof DefaultAgent agent2
                && agent2.getCustomMetadata() instanceof PulsarAgentNodeMetadata metadata2) {
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
}
