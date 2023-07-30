package com.datastax.oss.sga.cli.commands.gateway;

import com.datastax.oss.sga.cli.websocket.WebSocketClient;
import jakarta.websocket.CloseReason;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import lombok.SneakyThrows;
import picocli.CommandLine;

@CommandLine.Command(name = "produce",
        description = "Produce messages to a gateway")
public class ProduceGatewayCmd extends BaseGatewayCmd {

    record ProduceRequest(Object key, Object value, Map<String, String> headers) {
    }


    @CommandLine.Parameters(description = "Application ID")
    private String applicationId;
    @CommandLine.Parameters(description = "Gateway ID")
    private String gatewayId;

    @CommandLine.Option(names = {"-p", "--param"}, description = "Gateway parameters. Format: key=value")
    private Map<String, String> params;

    @CommandLine.Option(names = {"-c", "--credentials"}, description = "Credentials for the gateway. Required if the gateway requires authentication.")
    private String credentials;

    @CommandLine.Option(names = {"-v", "--value"}, description = "Message value")
    private String messageValue;

    @CommandLine.Option(names = {"-k", "--key"}, description = "Message key")
    private String messageKey;

    @CommandLine.Option(names = {"--header"}, description = "Messages headers. Format: key=value")
    private Map<String, String> headers;


    @Override
    @SneakyThrows
    public void run() {
        final String producePath = "%s/v1/produce/%s/%s/%s?%s"
                .formatted(getConfig().getApiGatewayUrl(), getConfig().getTenant(), applicationId, gatewayId,
                        computeQueryString(credentials, params, Map.of()));
        CountDownLatch countDownLatch = new CountDownLatch(1);
        try (final WebSocketClient client = new WebSocketClient(new WebSocketClient.Handler() {
            @Override
            public void onMessage(String msg) {
                log(msg);
                countDownLatch.countDown();
            }

            @Override
            public void onClose(CloseReason closeReason) {
                if (closeReason.getCloseCode() != CloseReason.CloseCodes.NORMAL_CLOSURE) {
                    err("Server closed connection with unexpected code: %s %s".formatted(closeReason.getCloseCode(),
                            closeReason.getReasonPhrase()));
                }
                countDownLatch.countDown();
            }

            @Override
            public void onError(Throwable throwable) {
                err("Connection error: %s".formatted(throwable.getMessage()));
            }
        })
                .connect(URI.create(producePath))) {
            final ProduceRequest produceRequest = new ProduceRequest(messageKey, messageValue, headers);
            final String json = messageMapper.writeValueAsString(produceRequest);
            client.send(json);
            countDownLatch.await();
        }
    }
}
