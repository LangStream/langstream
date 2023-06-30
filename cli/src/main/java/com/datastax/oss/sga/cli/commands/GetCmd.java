package com.datastax.oss.sga.cli.commands;

import lombok.SneakyThrows;
import picocli.CommandLine;

@CommandLine.Command(name = "get",
        description = "Get SGA application status")
public class GetCmd extends BaseCmd {

    @CommandLine.Parameters(description = "Name of the application")
    private String name;

    @Override
    @SneakyThrows
    public void run() {
        final String body = http(newGet("/applications/%s".formatted(name))).body();
        log(body);
    }
}
