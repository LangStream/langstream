package com.datastax.oss.sga.cli.commands.applications;

import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Flow;
import java.util.function.Consumer;
import lombok.SneakyThrows;
import picocli.CommandLine;

@CommandLine.Command(name = "logs",
        description = "Get SGA application logs")
public class GetApplicationLogsCmd extends BaseApplicationCmd {

    @CommandLine.Parameters(description = "Name of the application")
    private String name;

    @CommandLine.Option(names = {"-f"}, description = "Filter logs by the worker id")
    private List<String> filter;

    @Override
    @SneakyThrows
    public void run() {
        final String filterStr = filter == null ? "" : "?filter=" + String.join(",", filter);
        getHttpClient().sendAsync(newGet(tenantAppPath("/" + name + "/logs" + filterStr)), HttpResponse.BodyHandlers.ofByteArrayConsumer(
                        new Consumer<Optional<byte[]>>() {
                            @Override
                            public void accept(Optional<byte[]> bytes) {
                                if (bytes.isPresent()) {
                                    command.commandLine().getOut().print(new String(bytes.get(), StandardCharsets.UTF_8));
                                    command.commandLine().getOut().flush();
                                }

                            }
                        }))
                .thenApply(HttpResponse::statusCode)
                .thenAccept((status) -> {
                    if (status != 200) {
                        err("ERROR: %d status received".formatted(status));
                    }
                }).join();


    }
}
