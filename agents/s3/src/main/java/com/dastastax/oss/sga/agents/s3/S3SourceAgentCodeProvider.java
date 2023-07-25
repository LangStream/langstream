package com.dastastax.oss.sga.agents.s3;

import com.datastax.oss.sga.api.runner.code.AgentCode;
import com.datastax.oss.sga.api.runner.code.AgentCodeProvider;

public class S3SourceAgentCodeProvider implements AgentCodeProvider {
    @Override
    public boolean supports(String agentType) {
        return "s3-source".equals(agentType);
    }

    @Override
    public AgentCode createInstance(String agentType) {
        return new S3Source();
    }
}
