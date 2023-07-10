package com.datastax.oss.sga.pulsar;

import com.datastax.oss.sga.api.runtime.StreamingClusterRuntimeProvider;

public class PulsarStreamingClusterRuntimeProvider implements StreamingClusterRuntimeProvider {

    private PulsarStreamingClusterRuntime pulsarClusterRuntime = new PulsarStreamingClusterRuntime();

    @Override
    public  PulsarStreamingClusterRuntime getImplementation() {
        return pulsarClusterRuntime;
    }

    @Override
    public boolean supports(String type) {
        return "pulsar".equals(type);
    }
}
