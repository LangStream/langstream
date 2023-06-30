package com.datastax.oss.sga.cli;

import com.datastax.oss.sga.cli.commands.RootCmd;
import picocli.CommandLine;


public class SgaCLI {

    public static void main(String... args) {
        int exitCode = new CommandLine(new RootCmd()).execute(args);
        System.exit(exitCode);
    }
}