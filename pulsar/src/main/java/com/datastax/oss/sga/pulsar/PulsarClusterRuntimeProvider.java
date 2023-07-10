package com.datastax.oss.sga.pulsar;

import com.datastax.oss.sga.api.runtime.ClusterRuntime;
import com.datastax.oss.sga.api.runtime.ClusterRuntimeProvider;

public class PulsarClusterRuntimeProvider implements ClusterRuntimeProvider {

    private PulsarClusterRuntime pulsarClusterRuntime = new PulsarClusterRuntime();

    @Override
    public  ClusterRuntime getImplementation() {
        return pulsarClusterRuntime;
    }

    @Override
    public boolean supports(String type) {
        return "pulsar".equals(type);
    }
}
