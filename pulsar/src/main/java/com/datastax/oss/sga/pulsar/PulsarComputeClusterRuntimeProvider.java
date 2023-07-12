package com.datastax.oss.sga.pulsar;

import com.datastax.oss.sga.api.runtime.ComputeClusterRuntime;
import com.datastax.oss.sga.api.runtime.ComputeClusterRuntimeProvider;

public class PulsarComputeClusterRuntimeProvider implements ComputeClusterRuntimeProvider {

    private PulsarClusterRuntime pulsarClusterRuntime = new PulsarClusterRuntime();

    @Override
    public ComputeClusterRuntime getImplementation() {
        return pulsarClusterRuntime;
    }

    @Override
    public boolean supports(String type) {
        return "pulsar".equals(type);
    }
}
