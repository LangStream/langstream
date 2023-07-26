package com.datastax.oss.sga.runtime.agent;

import com.datastax.oss.sga.api.runner.code.AgentCode;
import com.datastax.oss.sga.api.runner.code.AgentCodeProvider;

public class CompositeAgentProcessorProvider implements AgentCodeProvider {
    @Override
    public boolean supports(String agentType) {
        return "composite-agent".equals(agentType);
    }

    @Override
    public AgentCode createInstance(String agentType) {
        return new CompositeAgentProcessor();
    }
}
