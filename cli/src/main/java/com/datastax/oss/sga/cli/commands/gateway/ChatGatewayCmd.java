package com.datastax.oss.sga.cli.commands.gateway;

import com.datastax.oss.sga.cli.websocket.WebSocketClient;
import com.fasterxml.jackson.core.JsonProcessingException;
import jakarta.websocket.CloseReason;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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
    @CommandLine.Option(names = {"-c", "--credentials"}, description = "Credentials for the gateway. Required if the gateway requires authentication.")
    private String credentials;

    @Override
    @SneakyThrows
    public void run() {
        final String consumePath = "%s/v1/consume/%s/%s/%s?%s"
                .formatted(getConfig().getApiGatewayUrl(), getConfig().getTenant(), applicationId, consumeFromGatewayId,
                        computeQueryString(credentials, params, Map.of("position", "earliest")));
        final String producePath = "%s/v1/produce/%s/%s/%s?%s"
                .formatted(getConfig().getApiGatewayUrl(), getConfig().getTenant(), applicationId, produceToGatewayId,
                        computeQueryString(credentials, params, Map.of()));

        AtomicBoolean waitingProduceResponse = new AtomicBoolean(false);
        AtomicBoolean waitingConsumeMessage = new AtomicBoolean(false);
        CountDownLatch consumerReady = new CountDownLatch(1);

        final AtomicReference<CompletableFuture<Void>> loop = new AtomicReference<>();

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
                if (closeReason.getCloseCode() != CloseReason.CloseCodes.NORMAL_CLOSURE) {
                    err("Server closed connection with unexpected code: %s %s".formatted(closeReason.getCloseCode(),
                            closeReason.getReasonPhrase()));
                }
                final CompletableFuture<Void> future = loop.get();
                future.cancel(true);
            }

            @Override
            public void onError(Throwable throwable) {
                err("Connection error: %s".formatted(throwable.getMessage()));
            }
        };

        final WebSocketClient.Handler consumeHandler = new WebSocketClient.Handler() {

            @Override
            public void onOpen() {
                consumerReady.countDown();
            }

            @Override
            @SneakyThrows
            public void onMessage(String msg) {
                waitingConsumeMessage.set(false);
                try {
                    final Map response = messageMapper.readValue(msg, Map.class);
                    final Map<String, Object> record = (Map<String, Object>) response.get("record");
                    logServer("\n");
                    logServer("Server:");
                    log(String.valueOf(record.get("value")));
                } catch (Throwable e) {
                    err("Error consuming message: %s".formatted(msg));
                }

            }

            @Override
            @SneakyThrows
            public void onClose(CloseReason closeReason) {
                if (closeReason.getCloseCode() != CloseReason.CloseCodes.NORMAL_CLOSURE) {
                    err("Server closed connection with unexpected code: %s %s".formatted(closeReason.getCloseCode(),
                            closeReason.getReasonPhrase()));
                }
                final CompletableFuture<Void> future = loop.get();
                future.cancel(true);
            }

            @Override
            public void onError(Throwable throwable) {
                err("Connection error: %s".formatted(throwable.getMessage()));
            }
        };
        try (final WebSocketClient consumeClient = new WebSocketClient(consumeHandler)
                .connect(URI.create(consumePath))) {
            consumerReady.await();
            log("Connected to %s".formatted(consumePath));
            try (final WebSocketClient produceClient = new WebSocketClient(produceHandler).connect(
                    URI.create(producePath))) {
                log("Connected to %s".formatted(producePath));


                final CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    try {
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
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                });
                loop.set(future);
                try {
                    future.join();
                } catch (CancellationException cancel) {
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
