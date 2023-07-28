package com.datastax.oss.testagents;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.runner.code.AgentCode;
import com.datastax.oss.sga.api.runner.code.AgentCodeProvider;
import com.datastax.oss.sga.api.runner.code.Record;
import com.datastax.oss.sga.api.runner.code.SingleRecordAgentProcessor;
import com.datastax.oss.sga.api.runtime.ComponentType;
import com.datastax.oss.sga.api.runtime.ComputeClusterRuntime;
import com.datastax.oss.sga.impl.common.AbstractAgentProvider;
import com.datastax.oss.sga.runtime.impl.k8s.KubernetesClusterRuntime;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Set;

@Slf4j
public class MockFailingAgentProcessorProvider extends AbstractAgentProvider{
    public MockFailingAgentProcessorProvider() {
        super(Set.of("mock-failing-processor"), List.of(KubernetesClusterRuntime.CLUSTER_TYPE));
    }

    @Override
    public boolean supports(String type, ComputeClusterRuntime clusterRuntime) {
        return super.supports(type, clusterRuntime);
    }

    @Override
    protected ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        return ComponentType.FUNCTION;
    }

}
