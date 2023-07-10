package com.dastastax.oss.sga.kafka;

import com.datastax.oss.sga.api.runtime.StreamingClusterRuntimeProvider;

public class KafkaStreamingClusterRuntimeProvider implements StreamingClusterRuntimeProvider {

    private KafkaStreamingClusterRuntime kafkaClusterRuntime = new KafkaStreamingClusterRuntime();

    @Override
    public KafkaStreamingClusterRuntime getImplementation() {
        return kafkaClusterRuntime;
    }

    @Override
    public boolean supports(String type) {
        return "kafka".equals(type);
    }
}
