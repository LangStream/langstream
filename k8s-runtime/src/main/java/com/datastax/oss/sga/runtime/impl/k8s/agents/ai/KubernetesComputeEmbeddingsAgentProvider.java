package com.datastax.oss.sga.runtime.impl.k8s.agents.ai;

import com.datastax.oss.sga.impl.agents.ai.ComputeEmbeddingsAgentProvider;
import com.datastax.oss.sga.runtime.impl.k8s.KubernetesClusterRuntime;

public class KubernetesComputeEmbeddingsAgentProvider extends ComputeEmbeddingsAgentProvider {

    public KubernetesComputeEmbeddingsAgentProvider() {
        super(KubernetesClusterRuntime.CLUSTER_TYPE);
    }

}
