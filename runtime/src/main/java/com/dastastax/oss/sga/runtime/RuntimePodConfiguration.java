package com.dastastax.oss.sga.runtime;

import com.datastax.oss.sga.api.model.StreamingCluster;
import java.util.Map;

public record RuntimePodConfiguration(Map<String, Object> input,
                                      Map<String, Object> output,
                                      Map<String, Object> agent,
                                      StreamingCluster streamingCluster) {
}
