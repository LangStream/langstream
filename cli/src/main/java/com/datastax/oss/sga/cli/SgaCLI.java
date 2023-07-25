package com.datastax.oss.sga.cli;

import com.datastax.oss.sga.cli.commands.RootCmd;
import picocli.CommandLine;


public class SgaCLI {

    public static void main(String... args) {
        int exitCode = execute(args);
        System.exit(exitCode);
    }

    public static int execute(String[] args) {
        final CommandLine cmdLine = new CommandLine(new RootCmd());
        CommandLine gen = cmdLine.getSubcommands().get("generate-completion");
        gen.getCommandSpec().usageMessage().hidden(true);
        int exitCode = cmdLine
                .setExecutionExceptionHandler((e, commandLine, parseResult) -> {
                    if (e.getMessage() != null) {
                        commandLine.getErr().println(commandLine.getColorScheme().errorText(e.getMessage()));
                    } else {
                        commandLine.getErr().println(commandLine.getColorScheme().errorText("Internal error " + e ));
                    }
                    return 1;
                })
                .execute(args);
        return exitCode;
    }
}