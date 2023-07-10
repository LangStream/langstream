package com.datastax.oss.sga.cli.commands;

import com.datastax.oss.sga.cli.commands.configure.ConfigureCmd;
import lombok.Getter;
import picocli.CommandLine;

@CommandLine.Command(name = "", mixinStandardHelpOptions = true, description = "Manage SGA",
        subcommands = { RootAppCmd.class, ConfigureCmd.class, RootTenantCmd.class})
public class RootCmd {


    @CommandLine.Option(names = { "--conf" }, description = "Sga CLI configuration file.")
    @Getter
    private String configPath;

    @CommandLine.Option(names = { "-v", "--verbose" }, defaultValue = "false", description = "Verbose mode. Helpful for troubleshooting.")
    @Getter
    private boolean verbose = false;
}
