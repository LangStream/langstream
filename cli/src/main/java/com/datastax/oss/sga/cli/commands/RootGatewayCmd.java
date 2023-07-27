package com.datastax.oss.sga.cli.commands;

import com.datastax.oss.sga.cli.commands.applications.AbstractDeployApplicationCmd;
import com.datastax.oss.sga.cli.commands.applications.DeleteApplicationCmd;
import com.datastax.oss.sga.cli.commands.applications.GetApplicationCmd;
import com.datastax.oss.sga.cli.commands.applications.GetApplicationLogsCmd;
import com.datastax.oss.sga.cli.commands.applications.ListApplicationCmd;
import com.datastax.oss.sga.cli.commands.gateway.ChatGatewayCmd;
import com.datastax.oss.sga.cli.commands.gateway.ConsumeGatewayCmd;
import com.datastax.oss.sga.cli.commands.gateway.ProduceGatewayCmd;
import lombok.Getter;
import picocli.CommandLine;

@CommandLine.Command(name = "gateway", mixinStandardHelpOptions = true, description = "Interact with a application gateway",
        subcommands = {
                ProduceGatewayCmd.class,
                ConsumeGatewayCmd.class,
                ChatGatewayCmd.class
        })
@Getter
public class RootGatewayCmd {
    @CommandLine.ParentCommand
    private RootCmd rootCmd;
}
