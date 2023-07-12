package com.datastax.oss.sga.impl.common;

import com.datastax.oss.sga.api.runtime.AgentNode;
import com.datastax.oss.sga.api.runtime.ComponentType;
import com.datastax.oss.sga.api.runtime.Connection;
import lombok.Getter;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

@Getter
@ToString
public class DefaultAgentNode implements AgentNode {
    private final String id;
    private final String agentType;
    private final ComponentType componentType;
    private Map<String, Object> configuration;
    private final Object customMetadata;

    private final Connection inputConnection;
    private Connection outputConnection;

    public DefaultAgentNode(String id, String agentType, ComponentType componentType, Map<String, Object> configuration, Object runtimeMetadata,
                            Connection inputConnection,
                            Connection outputConnection) {
        this.agentType = agentType;
        this.id = id;
        this.componentType = componentType;
        this.configuration = configuration;
        this.customMetadata = runtimeMetadata;
        this.inputConnection = inputConnection;
        this.outputConnection = outputConnection;
    }

    public <T> T getCustomMetadata() {
        return (T) customMetadata;
    }

    public void overrideConfigurationAfterMerge(Map<String, Object> newConfiguration, Connection newOutput) {
        this.configuration = new HashMap<>(newConfiguration);
        this.outputConnection = newOutput;
    }
}
