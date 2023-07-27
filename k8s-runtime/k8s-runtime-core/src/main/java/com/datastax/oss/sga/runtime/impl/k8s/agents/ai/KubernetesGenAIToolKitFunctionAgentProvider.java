package com.datastax.oss.sga.runtime.impl.k8s.agents.ai;

import com.datastax.oss.sga.impl.agents.ai.GenAIToolKitFunctionAgentProvider;
import com.datastax.oss.sga.runtime.impl.k8s.KubernetesClusterRuntime;

public class KubernetesGenAIToolKitFunctionAgentProvider extends GenAIToolKitFunctionAgentProvider {

    public KubernetesGenAIToolKitFunctionAgentProvider() {
        super(KubernetesClusterRuntime.CLUSTER_TYPE, GenAIToolKitFunctionAgentProvider.AGENT_TYPE);
    }

}
