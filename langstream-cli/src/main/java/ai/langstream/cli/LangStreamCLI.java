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
package ai.langstream.cli;

import ai.langstream.cli.commands.RootCmd;
import picocli.CommandLine;


public class LangStreamCLI {

    public static void main(String... args) {
        int exitCode = execute(args);
        System.exit(exitCode);
    }

    public static int execute(String[] args) {
        final CommandLine cmdLine = new CommandLine(new RootCmd());
        CommandLine gen = cmdLine.getSubcommands().get("generate-completion");
        gen.getCommandSpec().usageMessage().hidden(true);
        return cmdLine
                .setExecutionExceptionHandler((e, commandLine, parseResult) -> {
                    if (e.getMessage() != null) {
                        commandLine.getErr().println(commandLine.getColorScheme().errorText(e.getMessage()));
                        RootCmd rootCmd = cmdLine.getCommand();
                        if (rootCmd.isVerbose()) {
                            e.printStackTrace(commandLine.getErr());
                        }
                    } else {
                        commandLine.getErr().println(commandLine.getColorScheme().errorText("Internal error " + e ));
                    }
                    return 1;
                })
                .execute(args);
    }
}
