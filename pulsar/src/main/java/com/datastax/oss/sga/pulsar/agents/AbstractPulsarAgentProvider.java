package com.datastax.oss.sga.pulsar.agents;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.impl.common.AbstractAgentProvider;
import com.datastax.oss.sga.pulsar.PulsarName;
import com.datastax.oss.sga.pulsar.PulsarPhysicalApplicationInstance;
import java.util.List;
import java.util.Locale;

public abstract class AbstractPulsarAgentProvider extends AbstractAgentProvider {

    public AbstractPulsarAgentProvider(List<String> supportedTypes,
                                       List<String> supportedClusterTypes) {
        super(supportedTypes, supportedClusterTypes);
    }

    protected PulsarName computePulsarName(PulsarPhysicalApplicationInstance instance, AgentConfiguration agentConfiguration) {
        return new PulsarName(instance.getDefaultTenant(), instance.getDefaultNamespace(), sanitizeName(agentConfiguration));
    }

    private static String sanitizeName(AgentConfiguration agentConfiguration) {
        return agentConfiguration.getName()
                .replace(" ", "-")
                .replace(".", "-")
                .toLowerCase();
    }
}
