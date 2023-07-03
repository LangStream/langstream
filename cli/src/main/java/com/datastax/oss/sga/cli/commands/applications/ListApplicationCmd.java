package com.datastax.oss.sga.cli.commands.applications;

import com.datastax.oss.sga.cli.commands.BaseCmd;
import lombok.SneakyThrows;
import picocli.CommandLine;

@CommandLine.Command(name = "list",
        description = "List all SGA applications")
public class ListApplicationCmd extends BaseApplicationCmd {

    @Override
    @SneakyThrows
    public void run() {
        final String body = http(newGet("/applications")).body();
        log(body);
    }
}
