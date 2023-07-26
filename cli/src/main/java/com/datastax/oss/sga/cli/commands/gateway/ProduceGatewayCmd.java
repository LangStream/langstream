package com.datastax.oss.sga.cli.commands.gateway;

import com.datastax.oss.sga.cli.websocket.WebSocketClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.websocket.CloseReason;
import java.net.URI;
import java.util.Map;
import lombok.SneakyThrows;
import picocli.CommandLine;

@CommandLine.Command(name = "produce",
        description = "Produce messages to a gateway")
public class ProduceGatewayCmd extends BaseGatewayCmd {

    record ProduceRequest(Object key, Object value, Map<String, String> headers) {
    }
    static final ObjectMapper mapper = new ObjectMapper();

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
        try (final WebSocketClient client = new WebSocketClient(new WebSocketClient.Handler() {
            @Override
            public void onMessage(String msg) {
            }

            @Override
            public void onClose(CloseReason closeReason) {
            }

            @Override
            public void onError(Throwable throwable) {
                log("Connection error: %s".formatted(throwable.getMessage()));
            }
        })
                .connect(URI.create(producePath))) {
            final ProduceRequest produceRequest = new ProduceRequest(null, messageValue, Map.of());
            final String json = mapper.writeValueAsString(produceRequest);
            client.send(json);
            log("Produced 1 message to gateway %s/%s: %s".formatted(applicationId, gatewayId, json));
        }
    }
}
