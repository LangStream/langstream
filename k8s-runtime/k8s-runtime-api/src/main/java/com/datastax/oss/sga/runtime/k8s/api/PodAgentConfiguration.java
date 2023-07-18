package com.datastax.oss.sga.runtime.k8s.api;

import com.datastax.oss.sga.api.model.StreamingCluster;

import java.util.Map;

public record PodAgentConfiguration(Map<String, Object> input,
                                     Map<String, Object> output,
                                     AgentConfiguration agentConfiguration,
                                     StreamingCluster streamingCluster){
    public record AgentConfiguration(String agentId, String agentType, String componentType, Map<String, Object> configuration) {}
}
