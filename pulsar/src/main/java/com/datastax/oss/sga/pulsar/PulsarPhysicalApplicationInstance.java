package com.datastax.oss.sga.pulsar;

import com.datastax.oss.sga.api.model.Connection;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.runtime.AgentImplementation;
import com.datastax.oss.sga.api.runtime.ConnectionImplementation;
import com.datastax.oss.sga.api.runtime.PhysicalApplicationInstance;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class PulsarPhysicalApplicationInstance implements PhysicalApplicationInstance {

    private final Map<PulsarName, PulsarTopic> topics = new HashMap<>();
    private final Map<String, AgentImplementation> agents = new HashMap<>();

    private final String defaultTenant;
    private final String defaultNamespace;

    @Override
    public ConnectionImplementation getConnectionImplementation(Module module, Connection connection) {
        return null;
    }

    @Override
    public AgentImplementation getAgentImplementation(Module module, String id) {
        return agents.get(module.getId() + "#" + id);
    }

    public void registerAgent(Module module, String id, AgentImplementation agentImplementation) {
        agents.put(module.getId() + "#" + id, agentImplementation);
    }

}
