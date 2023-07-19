package com.datastax.oss.sga.runtime.agent.simple;

import com.datastax.oss.sga.api.runner.code.AgentCode;
import com.datastax.oss.sga.api.runner.code.AgentCodeProvider;
import com.datastax.oss.sga.api.runner.code.AgentFunction;
import com.datastax.oss.sga.api.runner.code.Record;

import java.util.ArrayList;
import java.util.List;

public class IdentityAgentProvider implements AgentCodeProvider {
    @Override
    public boolean supports(String agentType) {
        return "identity".equals(agentType);
    }

    @Override
    public AgentCode createInstance(String agentType) {
        return new IdentityAgentCode();
    }

    public static class IdentityAgentCode implements AgentFunction {
        @Override
        public List<Record> process(List<Record> record) {
            return new ArrayList<>(record);
        }
    }
}
