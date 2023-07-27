package com.datastax.oss.sga.cli.commands.gateway;

import com.datastax.oss.sga.cli.websocket.WebSocketClient;
import jakarta.websocket.CloseReason;
import java.net.URI;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.SneakyThrows;
import picocli.CommandLine;

@CommandLine.Command(name = "chat",
        description = "Produce and consume messages from gateway in a chat-like fashion")
public class ChatGatewayCmd extends BaseGatewayCmd {

    @CommandLine.Parameters(description = "Application ID")
    private String applicationId;
    @CommandLine.Option(names = {"-cg", "--consume-from-gateway"}, description = "Consume from gateway", required = true)
    private String consumeFromGatewayId;
    @CommandLine.Option(names = {"-pg", "--produce-to-gateway"}, description = "Produce to gateway", required = true)
    private String produceToGatewayId;
    @CommandLine.Option(names = {"-p", "--param"}, description = "Gateway parameters. Format: key=value")
    private Map<String, String> params;

    @Override
    @SneakyThrows
    public void run() {
        final String consumePath = "%s/v1/consume/%s/%s/%s?%s"
                .formatted(getConfig().getApiGatewayUrl(), getConfig().getTenant(), applicationId, consumeFromGatewayId,
                        computeQueryString(params));
        final String producePath = "%s/v1/produce/%s/%s/%s?%s"
                .formatted(getConfig().getApiGatewayUrl(), getConfig().getTenant(), applicationId, produceToGatewayId,
                        computeQueryString(params));

        AtomicBoolean waitingProduceResponse = new AtomicBoolean(false);
        AtomicBoolean waitingConsumeMessage = new AtomicBoolean(false);

        final WebSocketClient.Handler produceHandler = new WebSocketClient.Handler() {
            @Override
            @SneakyThrows
            public void onMessage(String msg) {
                final Map response = messageMapper.readValue(msg, Map.class);
                final String status = (String) response.getOrDefault("status", "OK");
                if (!"OK".equals(status)) {
                    err("Error sending message: %s".formatted(msg));
                } else {
                    logUser("✅");
                }
                waitingProduceResponse.set(false);
            }

            @Override
            public void onClose(CloseReason closeReason) {
            }

            @Override
            public void onError(Throwable throwable) {
                err("Connection error: %s".formatted(throwable.getMessage()));
            }
        };

        final WebSocketClient.Handler consumeHandler = new WebSocketClient.Handler() {
            @Override
            @SneakyThrows
            public void onMessage(String msg) {
                final Map response = messageMapper.readValue(msg, Map.class);
                final Map<String, Object> record = (Map<String, Object>) response.get("record");
                logServer("\n");
                logServer("Server:");
                log(String.valueOf(record.get("value")));
                waitingConsumeMessage.set(false);
            }

            @Override
            public void onClose(CloseReason closeReason) {
            }

            @Override
            public void onError(Throwable throwable) {
                err("Connection error: %s".formatted(throwable.getMessage()));
            }
        };
        try (final WebSocketClient consumeClient = new WebSocketClient(consumeHandler)
                .connect(URI.create(consumePath))) {
            log("Connected to %s".formatted(consumePath));
            try (final WebSocketClient produceClient = new WebSocketClient(produceHandler).connect(
                    URI.create(producePath))) {
                log("Connected to %s".formatted(producePath));

                Scanner scanner = new Scanner(System.in);
                while (true) {
                    logUser("\nYou:");
                    logUserNoNewLine("> ");
                    String line = scanner.nextLine();
                    produceClient.send(
                            messageMapper.writeValueAsString(
                            new ProduceGatewayCmd.ProduceRequest(null, line, Map.of()))
                    );
                    waitingProduceResponse.set(true);
                    waitingConsumeMessage.set(true);
                    while (waitingProduceResponse.get()) {
                        Thread.sleep(500);
                        if (waitingProduceResponse.get()) {
                            logUserNoNewLine(".");
                        }

                    }
                    while (waitingConsumeMessage.get()) {
                        Thread.sleep(500);
                        if (waitingConsumeMessage.get()) {
                            logUserNoNewLine(".");
                        }
                    }

                }
            }
        }
    }

    private void logUser(String message) {
        log("\u001B[34m" + message + "\u001B[0m");
        command.commandLine().getOut().flush();
    }

    private void logUserNoNewLine(String message) {
        command.commandLine().getOut().print("\u001B[34m" + message + "\u001B[0m");
        command.commandLine().getOut().flush();
    }
    private void logServer(String message) {
        log("\u001B[32m" + message + "\u001B[0m");
        command.commandLine().getOut().flush();
    }
}
