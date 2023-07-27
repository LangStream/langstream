package com.datastax.oss.sga.runtime.impl.k8s.agents;

import static com.datastax.oss.sga.runtime.impl.k8s.KubernetesClusterRuntime.CLUSTER_TYPE;
import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.runtime.ComponentType;
import com.datastax.oss.sga.impl.agents.AbstractComposableAgentProvider;

import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/**
 * Implements support for S3 Source Agents.
 */
@Slf4j
public class S3SourceAgentProvider extends AbstractComposableAgentProvider {

    public S3SourceAgentProvider() {
        super(Set.of("s3-source"), List.of(CLUSTER_TYPE, "none"));
    }

    @Override
    protected final ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        return ComponentType.SOURCE;
    }
}
