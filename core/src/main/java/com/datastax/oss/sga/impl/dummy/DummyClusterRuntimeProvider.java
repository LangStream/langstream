package com.datastax.oss.sga.impl.dummy;

import com.datastax.oss.sga.api.model.ApplicationInstance;
import com.datastax.oss.sga.api.model.Connection;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.runtime.ClusterRuntime;
import com.datastax.oss.sga.api.runtime.ClusterRuntimeProvider;
import com.datastax.oss.sga.api.runtime.ConnectionImplementation;
import com.datastax.oss.sga.api.runtime.PhysicalApplicationInstance;
import com.datastax.oss.sga.api.runtime.PluginsRegistry;
import com.datastax.oss.sga.api.runtime.StreamingClusterRuntime;

public class DummyClusterRuntimeProvider implements ClusterRuntimeProvider {
    @Override
    public boolean supports(String type) {
        return "none".equals(type);
    }

    @Override
    public ClusterRuntime getImplementation() {
        return new ClusterRuntime() {
            @Override
            public String getClusterType() {
                return "none";
            }

            @Override
            public PhysicalApplicationInstance createImplementation(ApplicationInstance applicationInstance, PluginsRegistry pluginsRegistry, StreamingClusterRuntime streamingClusterRuntime) {
                return new PhysicalApplicationInstance(applicationInstance);
            }

            @Override
            public ConnectionImplementation getConnectionImplementation(Module module, Connection connection, PhysicalApplicationInstance applicationInstance, StreamingClusterRuntime streamingClusterRuntime) {
                return null;
            }

            @Override
            public void deploy(ApplicationInstance logicalInstance, PhysicalApplicationInstance applicationInstance, StreamingClusterRuntime streamingClusterRuntime) {
            }

            @Override
            public void delete(ApplicationInstance logicalInstance, PhysicalApplicationInstance applicationInstance, StreamingClusterRuntime streamingClusterRuntime) {

            }
        };
    }
}
