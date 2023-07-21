package com.datastax.oss.sga.cli.commands;

import com.datastax.oss.sga.cli.commands.applications.DeleteApplicationCmd;
import com.datastax.oss.sga.cli.commands.applications.AbstractDeployApplicationCmd;
import com.datastax.oss.sga.cli.commands.applications.GetApplicationCmd;
import com.datastax.oss.sga.cli.commands.applications.GetApplicationLogsCmd;
import com.datastax.oss.sga.cli.commands.applications.ListApplicationCmd;
import lombok.Getter;
import picocli.CommandLine;

@CommandLine.Command(name = "apps", mixinStandardHelpOptions = true, description = "Manage SGA applications",
        subcommands = {
                AbstractDeployApplicationCmd.DeployApplicationCmd.class,
                AbstractDeployApplicationCmd.UpdateApplicationCmd.class,
                ListApplicationCmd.class,
                DeleteApplicationCmd.class,
                GetApplicationCmd.class,
                GetApplicationLogsCmd.class
        })
@Getter
public class RootAppCmd {
    @CommandLine.ParentCommand
    private RootCmd rootCmd;
}
