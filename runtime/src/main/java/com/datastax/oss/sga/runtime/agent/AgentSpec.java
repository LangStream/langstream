package com.datastax.oss.sga.runtime.agent;

import java.util.Map;

public record AgentSpec (ComponentType componentType,
                         String agentId,
                         String applicationId,
                         String agentType, Map<String, Object> configuration){
    public enum ComponentType {
        FUNCTION,
        SOURCE,
        SINK
    }
}
