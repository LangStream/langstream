package com.datastax.oss.sga.runtime.agent.simple;

import com.datastax.oss.sga.api.runner.code.AgentCode;
import com.datastax.oss.sga.api.runner.code.AgentCodeProvider;
import com.datastax.oss.sga.api.runner.code.AgentFunction;
import com.datastax.oss.sga.api.runner.code.Record;
import com.datastax.oss.sga.api.runner.code.SingleRecordAgentFunction;

import java.util.List;

public class NoOpAgentProvider implements AgentCodeProvider {
    @Override
    public boolean supports(String agentType) {
        return "noop".equals(agentType);
    }

    @Override
    public AgentCode createInstance(String agentType) {
        return new NoOpAgentCode();
    }

    private static class NoOpAgentCode extends SingleRecordAgentFunction {

        @Override
        public List<Record> processRecord(Record record) throws Exception {
            return List.of();
        }
    }
}
