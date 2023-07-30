package com.datastax.oss.sga.cli.commands.gateway;

import com.datastax.oss.sga.cli.websocket.WebSocketClient;
import jakarta.websocket.CloseReason;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import picocli.CommandLine;

@CommandLine.Command(name = "consume",
        description = "Consume messages from a gateway")
public class ConsumeGatewayCmd extends BaseGatewayCmd {

    @CommandLine.Parameters(description = "Application ID")
    private String applicationId;
    @CommandLine.Parameters(description = "Gateway ID")
    private String gatewayId;

    @CommandLine.Option(names = {"-p", "--param"}, description = "Gateway parameters. Format: key=value")
    private Map<String, String> params;
    @CommandLine.Option(names = {"-c", "--credentials"}, description = "Credentials for the gateway. Required if the gateway requires authentication.")
    private String credentials;

    @CommandLine.Option(names = {"--position"}, description = "Initial position of the consumer. \"latest\", \"earliest\" or a offset value. "
            + "The offset value can be retrieved after consuming a message of the same topic.")
    private String initialPosition;



    @Override
    @SneakyThrows
    public void run() {
        Map<String, String> options = new HashMap<>();
        if (initialPosition != null) {
            options.put("position", initialPosition);
        }


        final String consumePath = "%s/v1/consume/%s/%s/%s?%s"
                .formatted(getConfig().getApiGatewayUrl(), getConfig().getTenant(), applicationId, gatewayId,
                        computeQueryString(credentials, params, options));

        CountDownLatch latch = new CountDownLatch(1);
        try (final WebSocketClient client = new WebSocketClient(new WebSocketClient.Handler() {
            @Override
            public void onMessage(String msg) {
                log(msg);
            }

            @Override
            public void onClose(CloseReason closeReason) {
                latch.countDown();
            }

            @Override
            public void onError(Throwable throwable) {
                log("Connection error: %s".formatted(throwable.getMessage()));
                latch.countDown();
            }
        })
                .connect(URI.create(consumePath))) {
            log("Connected to %s".formatted(consumePath));
            latch.await();
        }
    }
}
