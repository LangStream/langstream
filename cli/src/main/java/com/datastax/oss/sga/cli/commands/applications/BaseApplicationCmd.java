package com.datastax.oss.sga.cli.commands.applications;

import com.datastax.oss.sga.cli.commands.BaseCmd;
import com.datastax.oss.sga.cli.commands.RootAppCmd;
import com.datastax.oss.sga.cli.commands.RootCmd;
import picocli.CommandLine;

public abstract class BaseApplicationCmd extends BaseCmd {

    @CommandLine.ParentCommand
    private RootAppCmd rootAppCmd;


    @Override
    protected RootCmd getRootCmd() {
        return rootAppCmd.getRootCmd();
    }


    protected String tenantAppPath(String uri) {
        final String tenant = getConfig().getTenant();
        if (tenant == null) {
            throw new IllegalStateException("Tenant not set. Run 'sga configure tenant <tenant>' to set it.");
        }
        debug("Using tenant: %s".formatted(tenant));
        return "/applications/%s%s".formatted(tenant, uri);
    }
}
