package com.datastax.oss.sga.impl.common;

import com.datastax.oss.sga.api.runtime.AgentNode;
import com.datastax.oss.sga.api.runtime.Connection;
import lombok.Getter;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

@Getter
@ToString
public class DefaultAgent implements AgentNode {
    private final String id;
    private final String type;
    private Map<String, Object> configuration;
    private final Object customMetadata;

    private final Connection inputConnection;
    private Connection outputConnection;

    public DefaultAgent(String id, String type, Map<String, Object> configuration, Object runtimeMetadata,
                        Connection inputConnection,
                        Connection outputConnection) {
        this.type = type;
        this.id = id;
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
