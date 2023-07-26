package com.datastax.oss.sga.cli.commands.gateway;

import com.datastax.oss.sga.cli.websocket.WebSocketClient;
import java.net.URI;
import lombok.SneakyThrows;
import picocli.CommandLine;

@CommandLine.Command(name = "produce",
        description = "Produce messages to a gateway")
public class ProduceGatewayCmd extends BaseGatewayCmd {

    @CommandLine.Parameters(description = "Application ID")
    private String applicationId;
    @CommandLine.Parameters(description = "Gateway ID")
    private String gatewayId;

    @CommandLine.Parameters(description = "Message value")
    private String messageValue;


    @Override
    @SneakyThrows
    public void run() {
        final String producePath = "%s/v1/produce/%s/%s/%s"
                .formatted(getConfig().getApiGatewayUrl(), getConfig().getTenant(), applicationId, gatewayId);
        try (final WebSocketClient client = new WebSocketClient(receivedMessage -> {})
                .connect(URI.create(producePath))) {
            client.send(messageValue);
        }
    }
}
