package com.datastax.oss.sga.ai.agents;

import com.datastax.oss.sga.api.runner.code.AgentCode;
import com.datastax.oss.sga.api.runner.code.AgentCodeProvider;

public class GenAIAgentCodeProvider implements AgentCodeProvider {
    @Override
    public boolean supports(String agentType) {
        return "ai-tools".equals(agentType);
    }

    @Override
    public AgentCode createInstance(String agentType) {
        return new GenAIToolKitAgent();
    }
}
