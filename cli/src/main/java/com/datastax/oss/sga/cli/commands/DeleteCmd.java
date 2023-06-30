package com.datastax.oss.sga.cli.commands;

import lombok.SneakyThrows;
import picocli.CommandLine;

@CommandLine.Command(name = "delete",
        description = "Delete an application")
public class DeleteCmd extends BaseCmd {

    @CommandLine.Parameters(description = "Name of the application")
    private String name;

    @Override
    @SneakyThrows
    public void run() {
        http(newDelete("/applications/%s".formatted(name)));
        log("Application deleted");
    }
}
