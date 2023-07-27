package com.datastax.oss.sga.cli.commands.gateway;

import com.datastax.oss.sga.cli.commands.BaseCmd;
import com.datastax.oss.sga.cli.commands.RootCmd;
import com.datastax.oss.sga.cli.commands.RootGatewayCmd;
import com.datastax.oss.sga.cli.commands.RootTenantCmd;
import picocli.CommandLine;

public abstract class BaseGatewayCmd extends BaseCmd {

    @CommandLine.ParentCommand
    private RootGatewayCmd cmd;

    @Override
    protected RootCmd getRootCmd() {
        return cmd.getRootCmd();
    }

}
