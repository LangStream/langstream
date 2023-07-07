package com.datastax.oss.sga.cli.commands.tenants;

import lombok.SneakyThrows;
import picocli.CommandLine;

@CommandLine.Command(name = "get",
        description = "Get tenant configuration")
public class GetTenantCmd extends BaseTenantCmd {

    @CommandLine.Parameters(description = "Name of the tenant")
    private String name;

    @Override
    @SneakyThrows
    public void run() {
        final String body = http(newGet(pathForTenant(name))).body();
        log(body);
    }
}
