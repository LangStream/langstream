package com.datastax.oss.sga.runtime.k8s.api;

import com.datastax.oss.sga.api.model.StreamingCluster;

import java.util.Map;

public record PodAgentConfiguration(
        String image,
        String imagePullPolicy,
        PodAgentConfiguration.ResourcesConfiguration resources,
        Map<String, Object> input,
        Map<String, Object> output,
        AgentConfiguration agentConfiguration,
        StreamingCluster streamingCluster,
        CodeStorageConfiguration codeStorage) {
    public record AgentConfiguration(String agentId, String agentType, String componentType, Map<String, Object> configuration) {}

    public record CodeStorageConfiguration(String codeStorageArchiveId) {}
    public record ResourcesConfiguration(Integer parallelism, Integer size) {}
}
