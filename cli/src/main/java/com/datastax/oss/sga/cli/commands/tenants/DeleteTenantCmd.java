package com.datastax.oss.sga.cli.commands.tenants;

import lombok.SneakyThrows;
import picocli.CommandLine;

@CommandLine.Command(name = "delete",
        description = "Delete a tenant")
public class DeleteTenantCmd extends BaseTenantCmd {

    @CommandLine.Parameters(description = "Name of the tenant")
    private String name;

    @Override
    @SneakyThrows
    public void run() {
        http(newDelete(pathForTenant(name)));
        log("Tenant %s deleted".formatted(name));
    }
}
