/**
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
