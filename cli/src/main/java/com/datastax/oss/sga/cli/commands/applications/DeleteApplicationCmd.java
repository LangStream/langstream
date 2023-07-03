package com.datastax.oss.sga.cli.commands.applications;

import com.datastax.oss.sga.cli.commands.BaseCmd;
import lombok.SneakyThrows;
import picocli.CommandLine;

@CommandLine.Command(name = "delete",
        description = "Delete an application")
public class DeleteApplicationCmd extends BaseApplicationCmd {

    @CommandLine.Parameters(description = "Name of the application")
    private String name;

    @Override
    @SneakyThrows
    public void run() {
        http(newDelete("/applications/%s".formatted(name)));
        log("Application %s deleted".formatted(name));
    }
}
