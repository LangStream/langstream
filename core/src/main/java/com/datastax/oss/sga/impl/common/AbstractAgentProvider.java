package com.datastax.oss.sga.impl.common;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.runtime.AgentNode;
import com.datastax.oss.sga.api.runtime.AgentNodeProvider;
import com.datastax.oss.sga.api.runtime.ComputeClusterRuntime;
import com.datastax.oss.sga.api.runtime.Connection;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.PluginsRegistry;
import com.datastax.oss.sga.api.runtime.StreamingClusterRuntime;
import lombok.Getter;
import lombok.ToString;

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

    @Getter
    @ToString
    public static class DefaultAgent implements AgentNode {
        private final String id;
        private final String type;
        private Map<String, Object> configuration;
        private final Object runtimeMetadata;

        private final Connection inputConnection;
        private Connection outputConnection;

        public DefaultAgent(String id, String type, Map<String, Object> configuration, Object runtimeMetadata,
                            Connection inputConnection,
                            Connection outputConnection) {
            this.type = type;
            this.id = id;
            this.configuration = configuration;
            this.runtimeMetadata = runtimeMetadata;
            this.inputConnection = inputConnection;
            this.outputConnection = outputConnection;
        }

        public <T> T getPhysicalMetadata() {
            return (T) runtimeMetadata;
        }

        public void overrideConfiguration(Map<String, Object> newConfiguration, Connection newOutput) {
            this.configuration = new HashMap<>(newConfiguration);
            this.outputConnection = newOutput;
        }
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

    protected Object computeAgentMetadata(AgentConfiguration agentConfiguration,
                                          ExecutionPlan physicalApplicationInstance,
                                          ComputeClusterRuntime clusterRuntime,
                                          StreamingClusterRuntime streamingClusterRuntime) {
        return clusterRuntime.computeAgentMetadata(agentConfiguration, physicalApplicationInstance, streamingClusterRuntime);
    }

    protected Map<String, Object> computeAgentConfiguration(AgentConfiguration agentConfiguration, Module module,
                                          ExecutionPlan physicalApplicationInstance,
                                          ComputeClusterRuntime clusterRuntime) {
        return agentConfiguration.getConfiguration();
    }

    @Override
    public AgentNode createImplementation(AgentConfiguration agentConfiguration,
                                          Module module,
                                          ExecutionPlan physicalApplicationInstance,
                                          ComputeClusterRuntime clusterRuntime, PluginsRegistry pluginsRegistry,
                                          StreamingClusterRuntime streamingClusterRuntime) {
        Object metadata = computeAgentMetadata(agentConfiguration, physicalApplicationInstance, clusterRuntime, streamingClusterRuntime);
        Map<String, Object> configuration = computeAgentConfiguration(agentConfiguration, module,
                physicalApplicationInstance, clusterRuntime);
        Connection input = computeInput(agentConfiguration, module, physicalApplicationInstance, clusterRuntime, streamingClusterRuntime);
        Connection output = computeOutput(agentConfiguration, module, physicalApplicationInstance, clusterRuntime, streamingClusterRuntime);
        return new DefaultAgent(agentConfiguration.getId(), agentConfiguration.getType(), configuration, metadata, input, output);
    }

    @Override
    public boolean supports(String type, ComputeClusterRuntime clusterRuntime) {
        return supportedTypes.contains(type) && supportedClusterTypes.contains(clusterRuntime.getClusterType());
    }
}
