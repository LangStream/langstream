package com.datastax.oss.sga.cli.commands.gateway;

import com.datastax.oss.sga.cli.websocket.WebSocketClient;
import java.net.URI;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import lombok.SneakyThrows;
import picocli.CommandLine;
@CommandLine.Command(name = "consume",
        description = "Consume messages from a gateway")
public class ConsumeGatewayCmd extends BaseGatewayCmd {

    @CommandLine.Parameters(description = "Application ID")
    private String applicationId;
    @CommandLine.Parameters(description = "Gateway ID")
    private String gatewayId;

    @CommandLine.Option(names = {"-n"}, description = "Number of messages to consume")
    private int numOfMessages = 1;


    private static final class StopException extends RuntimeException {
    }

    @Override
    @SneakyThrows
    public void run() {
        final String producePath = "%s/v1/consume/%s/%s/%s"
                .formatted(getConfig().getApiGatewayUrl(), getConfig().getTenant(), applicationId, gatewayId);

        AtomicInteger counter = new AtomicInteger(numOfMessages);

        try (final WebSocketClient client = new WebSocketClient(new Consumer<String>() {
            @Override
            public void accept(String s) {
                log(s);
                if (counter.decrementAndGet() == 0) {
                    throw new StopException();
                }

            }
        })
                .connect(URI.create(producePath))) {
        } catch (StopException e) {
            // do nothing
        }
    }
}
