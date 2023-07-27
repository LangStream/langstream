package com.datastax.oss.sga.runtime.impl.k8s.agents;

import com.datastax.oss.sga.impl.agents.AbstractCompositeAgentProvider;
import com.datastax.oss.sga.impl.noop.NoOpComputeClusterRuntimeProvider;
import com.datastax.oss.sga.runtime.impl.k8s.KubernetesClusterRuntime;

import java.util.List;

public class KubernetesCompositeAgentProvider extends AbstractCompositeAgentProvider {
    public KubernetesCompositeAgentProvider() {
        super(List.of(KubernetesClusterRuntime.CLUSTER_TYPE, NoOpComputeClusterRuntimeProvider.CLUSTER_TYPE));
    }
}
