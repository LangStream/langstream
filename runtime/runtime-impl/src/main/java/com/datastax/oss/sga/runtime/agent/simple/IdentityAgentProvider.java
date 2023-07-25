package com.datastax.oss.sga.runtime.agent.simple;

import com.datastax.oss.sga.api.runner.code.AgentCode;
import com.datastax.oss.sga.api.runner.code.AgentCodeProvider;
import com.datastax.oss.sga.api.runner.code.Record;
import com.datastax.oss.sga.api.runner.code.SingleRecordAgentFunction;

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

    public static class IdentityAgentCode extends SingleRecordAgentFunction {

        @Override
        public List<Record> processRecord(Record record) throws Exception {
            return List.of(record);
        }

    }
}
