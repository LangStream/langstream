/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.langstream.cli.commands.gateway;

import ai.langstream.cli.api.model.Gateways;
import ai.langstream.cli.websocket.WebSocketClient;
import com.fasterxml.jackson.core.JsonProcessingException;
import jakarta.websocket.CloseReason;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.SneakyThrows;
import picocli.CommandLine;

@CommandLine.Command(
        name = "chat",
        header = "Produce and consume messages from gateway in a chat-like fashion")
public class ChatGatewayCmd extends BaseGatewayCmd {

    @CommandLine.Parameters(description = "Application ID")
    private String applicationId;

    @CommandLine.Option(
            names = {"-cg", "--consume-from-gateway"},
            description = "Consume from gateway",
            required = true)
    private String consumeFromGatewayId;

    @CommandLine.Option(
            names = {"-pg", "--produce-to-gateway"},
            description = "Produce to gateway",
            required = true)
    private String produceToGatewayId;

    @CommandLine.Option(
            names = {"-p", "--param"},
            description = "Gateway parameters. Format: key=value")
    private Map<String, String> params;

    @CommandLine.Option(
            names = {"-c", "--credentials"},
            description =
                    "Credentials for the gateway. Required if the gateway requires authentication.")
    private String credentials;

    @CommandLine.Option(
            names = {"--connect-timeout"},
            description = "Connect timeout for WebSocket connections in seconds.")
    private long connectTimeoutSeconds = 0;

    @CommandLine.Option(
            names = {"-ac", "--admin-credentials"},
            description =
                    "Admin credentials for the gateway.")
    private String adminCredentials;

    @CommandLine.Option(
            names = {"-act", "--admin-credentials-type"},
            description = "Admin credentials type for the gateway.")
    private String adminCredentialsType;

    @CommandLine.Option(
            names = {"-aci", "--admin-credentials-input"},
            description = "Admin credentials type for the gateway.")
    private Map<String, String> adminCredentialsInputs;

    @Override
    @SneakyThrows
    public void run() {
        final Map<String, String> consumeGatewayOptions = Map.of("position", "latest");
        final String consumePath =
                validateGatewayAndGetUrl(
                        applicationId,
                        consumeFromGatewayId,
                        Gateways.Gateway.TYPE_CONSUME,
                        params,
                        consumeGatewayOptions,
                        credentials,
                        adminCredentials,
                        adminCredentialsType,
                        adminCredentialsInputs);
        final String producePath =
                validateGatewayAndGetUrl(
                        applicationId,
                        produceToGatewayId,
                        Gateways.Gateway.TYPE_PRODUCE,
                        params,
                        Map.of(),
                        credentials,
                        adminCredentials,
                        adminCredentialsType,
                        adminCredentialsInputs);

        final Duration connectTimeout =
                connectTimeoutSeconds > 0 ? Duration.ofSeconds(connectTimeoutSeconds) : null;

        AtomicBoolean waitingProduceResponse = new AtomicBoolean(false);
        AtomicBoolean waitingConsumeMessage = new AtomicBoolean(false);
        CountDownLatch consumerReady = new CountDownLatch(1);

        final AtomicReference<CompletableFuture<Void>> loop = new AtomicReference<>();

        final WebSocketClient.Handler produceHandler =
                new WebSocketClient.Handler() {

                    @Override
                    public void onOpen() {
                        log(String.format("Connected to %s", producePath));
                    }

                    @Override
                    @SneakyThrows
                    public void onMessage(String msg) {
                        final Map response = messageMapper.readValue(msg, Map.class);
                        final String status = (String) response.getOrDefault("status", "OK");
                        if (!"OK".equals(status)) {
                            err(String.format("Error sending message: %s", msg));
                        } else {
                            logUser("✅");
                        }
                        waitingProduceResponse.set(false);
                    }

                    @Override
                    public void onClose(CloseReason closeReason) {
                        if (closeReason.getCloseCode() != CloseReason.CloseCodes.NORMAL_CLOSURE) {
                            err(
                                    String.format(
                                            "Server closed connection with unexpected code: %s %s",
                                            closeReason.getCloseCode(),
                                            closeReason.getReasonPhrase()));
                        }
                        final CompletableFuture<Void> future = loop.get();
                        future.cancel(true);
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        err(String.format("Connection error: %s", throwable.getMessage()));
                    }
                };

        final WebSocketClient.Handler consumeHandler =
                new WebSocketClient.Handler() {

                    @Override
                    public void onOpen() {
                        log(String.format("Connected to %s", consumePath));
                        consumerReady.countDown();
                    }

                    @Override
                    @SneakyThrows
                    public void onMessage(String msg) {
                        try {
                            final Map response = messageMapper.readValue(msg, Map.class);
                            final Map<String, Object> record =
                                    (Map<String, Object>) response.get("record");
                            Map<String, String> headers =
                                    (Map<String, String>) record.get("headers");
                            boolean isStreamingOutput = false;
                            boolean isLastMessage = false;
                            int streamIndex = -1;
                            if (headers != null) {
                                String streamLastMessage = headers.get("stream-last-message");
                                if (streamLastMessage != null) {
                                    isStreamingOutput = true;
                                    isLastMessage = Boolean.parseBoolean(streamLastMessage + "");
                                    streamIndex =
                                            Integer.parseInt(
                                                    headers.getOrDefault("stream-index", "-1"));
                                }
                            }
                            if (isStreamingOutput) {
                                if (streamIndex == 1) {
                                    logServer("Server:");
                                }
                                logNoNewline(String.valueOf(record.get("value")));
                                if (isLastMessage) {
                                    logServer("\n");
                                    logServer(".");
                                    waitingConsumeMessage.set(false);
                                }
                            } else {
                                logServer("\n");
                                logServer("Server:");
                                log(String.valueOf(record.get("value")));
                                waitingConsumeMessage.set(false);
                            }
                        } catch (Throwable e) {
                            err(String.format("Error consuming message: %s", msg));
                        }
                    }

                    @Override
                    @SneakyThrows
                    public void onClose(CloseReason closeReason) {
                        if (closeReason.getCloseCode() != CloseReason.CloseCodes.NORMAL_CLOSURE) {
                            err(
                                    String.format(
                                            "Server closed connection with unexpected code: %s %s",
                                            closeReason.getCloseCode(),
                                            closeReason.getReasonPhrase()));
                        }
                        final CompletableFuture<Void> future = loop.get();
                        future.cancel(true);
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        err(String.format("Connection error: %s", throwable.getMessage()));
                    }
                };
        try (final WebSocketClient ignored =
                new WebSocketClient(consumeHandler)
                        .connect(URI.create(consumePath), connectTimeout)) {
            try (final WebSocketClient produceClient =
                    new WebSocketClient(produceHandler)
                            .connect(URI.create(producePath), connectTimeout)) {

                consumerReady.await();

                final CompletableFuture<Void> future =
                        CompletableFuture.runAsync(
                                () -> {
                                    try {
                                        Scanner scanner = new Scanner(System.in);
                                        while (true) {
                                            logUser("\nYou:");
                                            logUserNoNewLine("> ");
                                            String line = scanner.nextLine().trim();
                                            if (line.isBlank()) {
                                                continue;
                                            }
                                            produceClient.send(
                                                    messageMapper.writeValueAsString(
                                                            new ProduceGatewayCmd.ProduceRequest(
                                                                    null, line, Map.of())));
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
                    // ignore
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
