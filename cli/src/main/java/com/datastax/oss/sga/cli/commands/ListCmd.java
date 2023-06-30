package com.datastax.oss.sga.cli.commands;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import lombok.SneakyThrows;
import picocli.CommandLine;

@CommandLine.Command(name = "list",
        description = "List all SGA applications")
public class ListCmd extends BaseCmd {

    @Override
    @SneakyThrows
    public void run() {
        final HttpResponse<String> response = getHttpClient().send(
                HttpRequest.newBuilder()
                        .uri(URI.create("%s/api/applications".formatted(getBaseWebServiceUrl())))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            log("Error: " + response.statusCode());
        }
        log(response.body());

    }
}
