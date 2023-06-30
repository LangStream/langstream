package com.datastax.oss.sga.cli.commands;

import lombok.Getter;
import picocli.CommandLine;

@CommandLine.Command(name = "", mixinStandardHelpOptions = true, version = "checksum 4.0",
        description = "Prints the checksum (SHA-256 by default) of a file to STDOUT.",
        subcommands = { DeployCmd.class, ListCmd.class })
public class RootCmd {


    @CommandLine.Option(names = { "--conf" }, description = "Sga CLI configuration file.")
    @Getter
    private String configPath;

    @CommandLine.Option(names = { "-v", "--verbose" }, defaultValue = "false", description = "Verbose mode. Helpful for troubleshooting.")
    @Getter
    private boolean verbose = false;
}
