package com.datastax.oss.sga.impl.common;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.runtime.AgentNode;
import com.datastax.oss.sga.api.runtime.AgentNodeMetadata;
import com.datastax.oss.sga.api.runtime.AgentNodeProvider;
import com.datastax.oss.sga.api.runtime.ComponentType;
import com.datastax.oss.sga.api.runtime.ComputeClusterRuntime;
import com.datastax.oss.sga.api.runtime.Connection;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.PluginsRegistry;
import com.datastax.oss.sga.api.runtime.StreamingClusterRuntime;
import lombok.Getter;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
public abstract class AbstractAgentProvider implements AgentNodeProvider {

    protected final List<String> supportedTypes;
    protected final List<String> supportedClusterTypes;

    public AbstractAgentProvider(List<String> supportedTypes, List<String> supportedClusterTypes) {
        this.supportedTypes = Collections.unmodifiableList(supportedTypes);
        this.supportedClusterTypes = Collections.unmodifiableList(supportedClusterTypes);
    }

    protected Connection computeInput(AgentConfiguration agentConfiguration,
                                      Module module,
                                      ExecutionPlan physicalApplicationInstance,
                                      ComputeClusterRuntime clusterRuntime, StreamingClusterRuntime streamingClusterRuntime) {
        if (agentConfiguration.getInput() != null) {
            return clusterRuntime
                    .getConnectionImplementation(module, agentConfiguration.getInput(), physicalApplicationInstance, streamingClusterRuntime);
        } else {
            return null;
        }
    }

    protected Connection computeOutput(AgentConfiguration agentConfiguration,
                                       Module module,
                                       ExecutionPlan physicalApplicationInstance,
                                       ComputeClusterRuntime clusterRuntime, StreamingClusterRuntime streamingClusterRuntime) {
        if (agentConfiguration.getOutput() != null) {
            return clusterRuntime
                    .getConnectionImplementation(module, agentConfiguration.getOutput(), physicalApplicationInstance, streamingClusterRuntime);
        } else {
            return null;
        }
    }

    /**
     * Allow to override the component type
     * @param agentConfiguration
     * @return the component type
     */
    protected abstract ComponentType getComponentType(AgentConfiguration agentConfiguration);

    /**
     * Allow to override the agent type
     * @param agentConfiguration
     * @return the agent type
     */
    protected String getAgentType(AgentConfiguration agentConfiguration) {
        return agentConfiguration.getType();
    }

    protected AgentNodeMetadata computeAgentMetadata(AgentConfiguration agentConfiguration,
                                                     ExecutionPlan physicalApplicationInstance,
                                                     ComputeClusterRuntime clusterRuntime,
                                                     StreamingClusterRuntime streamingClusterRuntime) {
        return clusterRuntime.computeAgentMetadata(agentConfiguration, physicalApplicationInstance, streamingClusterRuntime);
    }

    protected Map<String, Object> computeAgentConfiguration(AgentConfiguration agentConfiguration, Module module,
                                          ExecutionPlan physicalApplicationInstance,
                                          ComputeClusterRuntime clusterRuntime) {
        return new HashMap<>(agentConfiguration.getConfiguration());
    }

    @Override
    public AgentNode createImplementation(AgentConfiguration agentConfiguration,
                                          Module module,
                                          ExecutionPlan physicalApplicationInstance,
                                          ComputeClusterRuntime clusterRuntime, PluginsRegistry pluginsRegistry,
                                          StreamingClusterRuntime streamingClusterRuntime) {
        Object metadata = computeAgentMetadata(agentConfiguration, physicalApplicationInstance, clusterRuntime, streamingClusterRuntime);
        String agentType = getAgentType(agentConfiguration);
        ComponentType componentType = getComponentType(agentConfiguration);
        Map<String, Object> configuration = computeAgentConfiguration(agentConfiguration, module,
                physicalApplicationInstance, clusterRuntime);
        Connection input = computeInput(agentConfiguration, module, physicalApplicationInstance, clusterRuntime, streamingClusterRuntime);
        Connection output = computeOutput(agentConfiguration, module, physicalApplicationInstance, clusterRuntime, streamingClusterRuntime);
        return new DefaultAgentNode(agentConfiguration.getId(),
                agentType,
                componentType,
                configuration,
                metadata, input, output);
    }

    @Override
    public boolean supports(String type, ComputeClusterRuntime clusterRuntime) {
        return supportedTypes.contains(type) && supportedClusterTypes.contains(clusterRuntime.getClusterType());
    }
}
