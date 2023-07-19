package com.datastax.oss.sga.runtime.agent.python;

import com.datastax.oss.sga.api.runner.code.AgentCode;
import com.datastax.oss.sga.api.runner.code.AgentCodeProvider;

public class PythonCodeAgentProvider implements AgentCodeProvider {
    @Override
    public boolean supports(String agentType) {
        switch (agentType) {
            case "python-source":
            case "python-sink":
            case "python-function":
                return true;
            default:
                return false;
        }
    }

    @Override
    public AgentCode createInstance(String agentType) {
        return new PythonAgentPlaceHolder();
    }

    private static class PythonAgentPlaceHolder implements AgentCode {
    }

    public static boolean isPythonCodeAgent(AgentCode agentCode) {
        return agentCode instanceof PythonAgentPlaceHolder;
    }
}
