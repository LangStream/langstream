package com.datastax.oss.sga.cli.commands.tenants;

import lombok.SneakyThrows;
import picocli.CommandLine;

@CommandLine.Command(name = "list",
        description = "List all tenants")
public class ListTenantCmd extends BaseTenantCmd {

    @Override
    @SneakyThrows
    public void run() {
        final String body = http(newGet("/tenants")).body();
        log(body);
    }
}
