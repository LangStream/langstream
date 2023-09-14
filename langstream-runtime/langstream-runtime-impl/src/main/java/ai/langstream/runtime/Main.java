/*
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
package ai.langstream.runtime;

import ai.langstream.runtime.agent.AgentCodeDownloaderStarter;
import ai.langstream.runtime.agent.AgentRunnerStarter;
import ai.langstream.runtime.application.ApplicationSetupRunnerStarter;
import ai.langstream.runtime.deployer.RuntimeDeployerStarter;
import java.util.List;

public class Main {

    private static final List<String> COMMANDS =
            List.of(
                    "agent-runtime",
                    "agent-code-download",
                    "deployer-runtime",
                    "application-setup");

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Unknown command. Only " + COMMANDS + " are supported.");
            System.exit(1);
        }
        String command = args[0];
        final String[] newArgs = new String[args.length - 1];
        System.arraycopy(args, 1, newArgs, 0, newArgs.length);
        switch (command) {
            case "agent-runtime" -> AgentRunnerStarter.main(newArgs);
            case "agent-code-download" -> AgentCodeDownloaderStarter.main(newArgs);
            case "deployer-runtime" -> RuntimeDeployerStarter.main(newArgs);
            case "application-setup" -> ApplicationSetupRunnerStarter.main(newArgs);
            default -> {
                System.err.println("Unknown command. Only " + COMMANDS + " are supported.");
                System.exit(1);
            }
        }
    }
}
