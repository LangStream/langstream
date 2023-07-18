package com.datastax.oss.sga.runtime.api.agent;

import com.datastax.oss.sga.api.model.StreamingCluster;
import java.util.Map;

public record RuntimePodConfiguration(Map<String, Object> input,
                                      Map<String, Object> output,
                                      AgentSpec agent,
                                      StreamingCluster streamingCluster,
                                      CodeStorageConfig codeStorage) {
}
