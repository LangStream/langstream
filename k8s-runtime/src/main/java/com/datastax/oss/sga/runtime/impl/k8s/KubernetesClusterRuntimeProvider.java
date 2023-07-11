package com.datastax.oss.sga.runtime.impl.k8s;

import com.datastax.oss.sga.api.runtime.ClusterRuntime;
import com.datastax.oss.sga.api.runtime.ClusterRuntimeProvider;

public class KubernetesClusterRuntimeProvider implements ClusterRuntimeProvider {
    @Override
    public boolean supports(String type) {
        return "kubernetes".equals(type);
    }

    @Override
    public ClusterRuntime getImplementation() {
        return new KubernetesClusterRuntime();
    }
}
