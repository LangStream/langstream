package com.datastax.oss.sga.cli;

import com.datastax.oss.sga.cli.commands.RootCmd;
import picocli.CommandLine;


public class SgaCLI {

    public static void main(String... args) {
        int exitCode = execute(args);
        System.exit(exitCode);
    }

    public static int execute(String[] args) {
        int exitCode = new CommandLine(new RootCmd())
                .setExecutionExceptionHandler((e, commandLine, parseResult) -> {
                    if (e.getMessage() != null) {
                        commandLine.getErr().println(commandLine.getColorScheme().errorText(e.getMessage()));
                    }
                    return 1;
                })
                .execute(args);
        return exitCode;
    }
}