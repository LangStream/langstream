package com.datastax.oss.sga.runtime;

import com.datastax.oss.sga.api.runner.code.AgentCode;
import com.datastax.oss.sga.runtime.agent.AgentCodeDownloader;
import com.datastax.oss.sga.runtime.agent.AgentRunner;
import com.datastax.oss.sga.runtime.deployer.RuntimeDeployer;

public class Main {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Unknown command. Only ['agent-runtime', 'deployer-runtime', 'agent-code-download']");
            System.exit(1);
        }
        String command = args[0];
        final String[] newArgs = new String[args.length - 1];
        System.arraycopy(args, 1, newArgs, 0, newArgs.length);
        switch (command) {
            case "agent-runtime":
                AgentRunner.main(newArgs);
                break;
            case "agent-code-download":
                AgentCodeDownloader.main(newArgs);
                break;
            case "deployer-runtime":
                RuntimeDeployer.main(newArgs);
                break;
            default: {
                System.err.println("Unknown command. Only ['agent-runtime', 'deployer-runtime', 'agent-code-download']");
                System.exit(1);
            }
        }

    }
}
