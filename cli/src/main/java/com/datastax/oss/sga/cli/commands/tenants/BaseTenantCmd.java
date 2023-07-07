package com.datastax.oss.sga.cli.commands.tenants;

import com.datastax.oss.sga.cli.commands.BaseCmd;
import com.datastax.oss.sga.cli.commands.RootCmd;
import com.datastax.oss.sga.cli.commands.RootTenantCmd;
import picocli.CommandLine;

public abstract class BaseTenantCmd extends BaseCmd {

    @CommandLine.ParentCommand
    private RootTenantCmd cmd;

    @Override
    protected RootCmd getRootCmd() {
        return cmd.getRootCmd();
    }

    protected String pathForTenant(String tenant) {
        return "/tenants/%s".formatted(tenant);
    }
}
