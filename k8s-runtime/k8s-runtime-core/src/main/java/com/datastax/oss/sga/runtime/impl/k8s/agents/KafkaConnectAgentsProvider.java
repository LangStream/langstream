package com.datastax.oss.sga.runtime.impl.k8s.agents;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.runtime.ComponentType;
import com.datastax.oss.sga.impl.common.AbstractAgentProvider;
import com.datastax.oss.sga.runtime.impl.k8s.KubernetesClusterRuntime;

import java.util.List;
import java.util.Set;

public class KafkaConnectAgentsProvider extends AbstractAgentProvider {

    public KafkaConnectAgentsProvider() {
        super(Set.of("sink", "source"), List.of(KubernetesClusterRuntime.CLUSTER_TYPE));
    }

    @Override
    protected ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        switch (agentConfiguration.getType()) {
            case "sink":
                return ComponentType.SINK;
                case "source":
                    return ComponentType.SOURCE;
            default:
                throw new IllegalStateException();
        }
    }

}
