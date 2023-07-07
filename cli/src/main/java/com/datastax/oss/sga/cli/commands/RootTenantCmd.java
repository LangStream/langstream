package com.datastax.oss.sga.cli.commands;

import com.datastax.oss.sga.cli.commands.applications.DeleteApplicationCmd;
import com.datastax.oss.sga.cli.commands.applications.DeployApplicationCmd;
import com.datastax.oss.sga.cli.commands.applications.GetApplicationCmd;
import com.datastax.oss.sga.cli.commands.applications.ListApplicationCmd;
import com.datastax.oss.sga.cli.commands.tenants.DeleteTenantCmd;
import com.datastax.oss.sga.cli.commands.tenants.GetTenantCmd;
import com.datastax.oss.sga.cli.commands.tenants.ListTenantCmd;
import com.datastax.oss.sga.cli.commands.tenants.PutTenantCmd;
import lombok.Getter;
import picocli.CommandLine;

@CommandLine.Command(name = "tenants", mixinStandardHelpOptions = true, description = "Manage SGA tenants",
        subcommands = {PutTenantCmd.class, DeleteTenantCmd.class, ListTenantCmd.class, GetTenantCmd.class})
@Getter
public class RootTenantCmd {
    @CommandLine.ParentCommand
    private RootCmd rootCmd;
}
