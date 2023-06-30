package com.datastax.oss.sga.pulsar.agents.ai;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.model.ApplicationInstance;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.Resource;
import com.datastax.oss.sga.api.runtime.ClusterRuntime;
import com.datastax.oss.sga.api.runtime.ConnectionImplementation;
import com.datastax.oss.sga.api.runtime.PhysicalApplicationInstance;
import com.datastax.oss.sga.pulsar.PulsarClusterRuntime;
import com.datastax.oss.sga.pulsar.PulsarTopic;
import com.datastax.oss.sga.pulsar.agents.AbstractPulsarFunctionAgentProvider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GenAIToolKitFunctionAgentProvider extends AbstractPulsarFunctionAgentProvider {

    public GenAIToolKitFunctionAgentProvider(String stepType) {
        super(List.of(stepType), List.of(PulsarClusterRuntime.CLUSTER_TYPE));
    }

    @Override
    protected String getFunctionType(AgentConfiguration agentConfiguration) {
        // https://github.com/datastax/pulsar-transformations/tree/master/pulsar-ai-tools
        return "ai-tools";
    }

    @Override
    protected String getFunctionClassname(AgentConfiguration agentConfiguration) {
        return null;
    }

    protected void generateSteps(Map<String, Object> originalConfiguration, List<Map<String, Object>> steps) {
    }

    private void generateOpenAIConfiguration(ApplicationInstance applicationInstance, Map<String, Object> configuration) {
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
                                                            PhysicalApplicationInstance physicalApplicationInstance,
                                                            ClusterRuntime clusterRuntime) {
        Map<String, Object> originalConfiguration = super.computeAgentConfiguration(agentConfiguration, module, physicalApplicationInstance, clusterRuntime);
        Map<String, Object> configuration = new HashMap<>();

        generateOpenAIConfiguration(physicalApplicationInstance.getApplicationInstance(), configuration);

        List<Map<String, Object>> steps = new ArrayList<>();
        configuration.put("steps", steps);
        generateSteps(originalConfiguration, steps);
        return configuration;
    }
}
