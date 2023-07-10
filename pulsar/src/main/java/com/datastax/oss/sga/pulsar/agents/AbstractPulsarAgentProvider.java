package com.datastax.oss.sga.pulsar.agents;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.runtime.PhysicalApplicationInstance;
import com.datastax.oss.sga.impl.common.AbstractAgentProvider;
import com.datastax.oss.sga.pulsar.PulsarClusterRuntimeConfiguration;
import com.datastax.oss.sga.pulsar.PulsarName;

import java.util.List;

import static com.datastax.oss.sga.pulsar.PulsarClusterRuntime.getPulsarClusterRuntimeConfiguration;

public abstract class AbstractPulsarAgentProvider extends AbstractAgentProvider {

    public AbstractPulsarAgentProvider(List<String> supportedTypes,
                                       List<String> supportedClusterTypes) {
        super(supportedTypes, supportedClusterTypes);
    }

    protected PulsarName computePulsarName(PhysicalApplicationInstance instance, AgentConfiguration agentConfiguration) {
        PulsarClusterRuntimeConfiguration pulsarClusterRuntimeConfiguration = getPulsarClusterRuntimeConfiguration(instance.getApplicationInstance().getInstance().streamingCluster());
        return new PulsarName(pulsarClusterRuntimeConfiguration.getDefaultTenant(),
                pulsarClusterRuntimeConfiguration.getDefaultNamespace(), sanitizeName(agentConfiguration));
    }

    private static String sanitizeName(AgentConfiguration agentConfiguration) {
        return agentConfiguration.getName()
                .replace(" ", "-")
                .replace(".", "-")
                .toLowerCase();
    }
}
