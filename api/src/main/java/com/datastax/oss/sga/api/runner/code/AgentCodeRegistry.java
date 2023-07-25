package com.datastax.oss.sga.api.runner.code;

import java.util.Objects;
import java.util.ServiceLoader;

/**
 * The runtime registry is a singleton that holds all the runtime information about the
 * possible implementations of the SGA API.
 */
public class AgentCodeRegistry {

    public AgentCode getAgentCode(String agentType) {
        Objects.requireNonNull(agentType, "agentType cannot be null");
        ServiceLoader<AgentCodeProvider> loader = ServiceLoader.load(AgentCodeProvider.class);
        ServiceLoader.Provider<AgentCodeProvider> agentCodeProviderProvider = loader
                .stream()
                .filter(p -> p.get().supports(agentType))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No AgentCodeProvider found for type " + agentType));

        return agentCodeProviderProvider.get().createInstance(agentType);
    }

}
