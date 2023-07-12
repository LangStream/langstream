package com.datastax.oss.sga.pulsar.agents;

import com.datastax.oss.sga.api.runtime.AgentNodeMetadata;
import com.datastax.oss.sga.api.runtime.ComponentType;
import com.datastax.oss.sga.pulsar.PulsarName;
import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class PulsarAgentNodeMetadata implements AgentNodeMetadata {
    private final PulsarName pulsarName;
    private final ComponentType componentType;
    private final String agentType;
}
