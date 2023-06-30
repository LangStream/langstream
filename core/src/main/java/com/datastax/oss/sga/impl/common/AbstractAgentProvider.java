package com.datastax.oss.sga.impl.common;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.runtime.AgentImplementation;
import com.datastax.oss.sga.api.runtime.AgentImplementationProvider;
import com.datastax.oss.sga.api.runtime.ClusterRuntime;
import com.datastax.oss.sga.api.runtime.ConnectionImplementation;
import com.datastax.oss.sga.api.runtime.PhysicalApplicationInstance;
import com.datastax.oss.sga.api.runtime.PluginsRegistry;
import lombok.Getter;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Getter
public abstract class AbstractAgentProvider implements AgentImplementationProvider {

    protected final List<String> supportedTypes;
    protected final List<String> supportedClusterTypes;

    public AbstractAgentProvider(List<String> supportedTypes, List<String> supportedClusterTypes) {
        this.supportedTypes = Collections.unmodifiableList(supportedTypes);
        this.supportedClusterTypes = Collections.unmodifiableList(supportedClusterTypes);
    }

    @Getter
    public static class DefaultAgentImplementation implements AgentImplementation {
        private final String type;
        private final Map<String, Object> configuration;
        private final Object runtimeMetadata;

        private final ConnectionImplementation inputConnection;
        private final ConnectionImplementation outputConnection;

        public DefaultAgentImplementation(String type, Map<String, Object> configuration, Object runtimeMetadata,
                                          ConnectionImplementation inputConnection,
                                          ConnectionImplementation outputConnection) {
            this.type = type;
            this.configuration = configuration;
            this.runtimeMetadata = runtimeMetadata;
            this.inputConnection = inputConnection;
            this.outputConnection = outputConnection;
        }

        public <T> T getPhysicalMetadata() {
            return (T) runtimeMetadata;
        }
    }

    protected ConnectionImplementation computeInput(AgentConfiguration agentConfiguration,
                                          Module module,
                                          PhysicalApplicationInstance physicalApplicationInstance,
                                          ClusterRuntime clusterRuntime) {
        if (agentConfiguration.getInput() != null) {
            return physicalApplicationInstance
                    .getConnectionImplementation(module, agentConfiguration.getInput());
        } else {
            return null;
        }
    }

    protected ConnectionImplementation computeOutput(AgentConfiguration agentConfiguration,
                                                    Module module,
                                                    PhysicalApplicationInstance physicalApplicationInstance,
                                                    ClusterRuntime clusterRuntime) {
        if (agentConfiguration.getOutput() != null) {
            return physicalApplicationInstance
                    .getConnectionImplementation(module, agentConfiguration.getOutput());
        } else {
            return null;
        }
    }

    protected Object computeAgentMetadata(AgentConfiguration agentConfiguration,
                                          PhysicalApplicationInstance physicalApplicationInstance,
                                          ClusterRuntime clusterRuntime) {
        return clusterRuntime.computeAgentMetadata(agentConfiguration, physicalApplicationInstance);
    }

    protected Map<String, Object> computeAgentConfiguration(AgentConfiguration agentConfiguration, Module module,
                                          PhysicalApplicationInstance physicalApplicationInstance,
                                          ClusterRuntime clusterRuntime) {
        return agentConfiguration.getConfiguration();
    }

    @Override
    public AgentImplementation createImplementation(AgentConfiguration agentConfiguration,
                                                    Module module,
                                                    PhysicalApplicationInstance physicalApplicationInstance,
                                                    ClusterRuntime clusterRuntime, PluginsRegistry pluginsRegistry) {
        Object metadata = computeAgentMetadata(agentConfiguration, physicalApplicationInstance, clusterRuntime);
        Map<String, Object> configuration = computeAgentConfiguration(agentConfiguration, module,
                physicalApplicationInstance, clusterRuntime);
        ConnectionImplementation input = computeInput(agentConfiguration, module, physicalApplicationInstance, clusterRuntime);
        ConnectionImplementation output = computeOutput(agentConfiguration, module, physicalApplicationInstance, clusterRuntime);
        return new DefaultAgentImplementation(agentConfiguration.getType(), configuration, metadata, input, output);
    }

    @Override
    public boolean supports(String type, ClusterRuntime<?> clusterRuntime) {
        return supportedTypes.contains(type) && supportedClusterTypes.contains(clusterRuntime.getClusterType());
    }
}
