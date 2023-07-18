package com.datastax.oss.sga.runtime.impl.k8s;

import com.datastax.oss.sga.api.runtime.ComputeClusterRuntime;
import com.datastax.oss.sga.api.runtime.ComputeClusterRuntimeProvider;

public class KubernetesComputeClusterRuntimeProvider implements ComputeClusterRuntimeProvider {
    @Override
    public boolean supports(String type) {
        return "kubernetes".equals(type);
    }

    @Override
    public ComputeClusterRuntime getImplementation() {
        return new KubernetesClusterRuntime();
    }
}
