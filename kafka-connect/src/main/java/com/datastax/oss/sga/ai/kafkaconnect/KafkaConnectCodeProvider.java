package com.datastax.oss.sga.ai.kafkaconnect;

import com.datastax.oss.sga.api.runner.code.AgentCode;
import com.datastax.oss.sga.api.runner.code.AgentCodeProvider;

public class KafkaConnectCodeProvider implements AgentCodeProvider {
    @Override
    public boolean supports(String agentType) {
        return "sink".equals(agentType) || "source".equals(agentType);
    }

    @Override
    public AgentCode createInstance(String agentType) {
        switch (agentType) {
            case "sink":
                return new KafkaConnectSinkAgent();
            case "source":
                return new KafkaConnectSourceAgent();
            default:
                throw new IllegalStateException();
        }

    }
}
