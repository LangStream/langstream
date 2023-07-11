package com.datastax.oss.sga.runtime.impl.k8s;

import com.datastax.oss.sga.api.model.StreamingCluster;

import java.util.Map;

public record PodAgentConfiguration(Map<String, Object> input,
                                     Map<String, Object> output,
                                     Map<String, Object> agentConfiguration,
                                     StreamingCluster streamingCluster){
}
