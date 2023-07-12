package com.dastastax.oss.sga.runtime;

import java.util.Map;

public record AgentSpec (ComponentType componentType, String agentType, Map<String, Object> configuration){
    public enum ComponentType {
        FUNCTION,
        SOURCE,
        SINK
    }
}
