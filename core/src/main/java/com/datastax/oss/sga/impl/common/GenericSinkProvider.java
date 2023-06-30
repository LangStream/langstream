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
public abstract class GenericSinkProvider implements AgentImplementationProvider {

    protected final List<String> supportedTypes;
    protected final List<String> supportedClusterTypes;

    public GenericSinkProvider(List<String> supportedTypes, List<String> supportedClusterTypes) {
        this.supportedTypes = Collections.unmodifiableList(supportedTypes);
        this.supportedClusterTypes = Collections.unmodifiableList(supportedClusterTypes);
    }

    @Getter
    public static class GenericSink implements AgentImplementation {
        private final String type;
        private final Map<String, Object> configuration;
        private final Object runtimeMetadata;

        private final ConnectionImplementation inputConnection;

        public GenericSink(String type, Map<String, Object> configuration, Object runtimeMetadata,
                           ConnectionImplementation inputConnection) {
            this.type = type;
            this.configuration = configuration;
            this.runtimeMetadata = runtimeMetadata;
            this.inputConnection = inputConnection;
        }

        public <T> T getPhysicalMetadata() {
            return (T) runtimeMetadata;
        }
    }

    protected ConnectionImplementation computeInput(AgentConfiguration agentConfiguration,
                                          Module module,
                                          PhysicalApplicationInstance physicalApplicationInstance,
                                          ClusterRuntime clusterRuntime) {
        return physicalApplicationInstance
                .getConnectionImplementation(module, agentConfiguration.getInput());
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
        return new GenericSink(agentConfiguration.getType(), configuration, metadata, input);
    }

    @Override
    public boolean supports(String type, ClusterRuntime<?> clusterRuntime) {
        return supportedTypes.contains(type) && supportedClusterTypes.contains(clusterRuntime.getClusterType());
    }
}
