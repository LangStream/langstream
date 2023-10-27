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
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import lombok.SneakyThrows;
import picocli.CommandLine;

@CommandLine.Command(
        name = "chat",
        header = "Produce and consume messages from gateway in a chat-like fashion")
public class ChatGatewayCmd extends BaseGatewayCmd {

    @CommandLine.Parameters(description = "Application ID")
    private String applicationId;

    @CommandLine.Option(
            names = {"-g", "--chat-gateway"},
            description = "Use a 'chat' gateway")
    private String chatGatewayId;

    @CommandLine.Option(
            names = {"-cg", "--consume-from-gateway"},
            description = "Consume from gateway")
    private String consumeFromGatewayId;

    @CommandLine.Option(
            names = {"-pg", "--produce-to-gateway"},
            description = "Produce to gateway")
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
            names = {"-tc", "--test-credentials"},
            description = "Test credentials for the gateway.")
    private String testCredentials;

    @CommandLine.Option(
            names = {"--connect-timeout"},
            description = "Connect timeout for WebSocket connections in seconds.")
    private long connectTimeoutSeconds = 0;

    @Override
    @SneakyThrows
    public void run() {
        GatewayConnection connection = createGatewayConnection();

        AtomicBoolean waitingProduceResponse = new AtomicBoolean(false);
        AtomicBoolean waitingConsumeMessage = new AtomicBoolean(false);
        AtomicBoolean isStreamingOutput = new AtomicBoolean(false);

        final AtomicReference<CompletableFuture<Void>> loop = new AtomicReference<>();

        final Consumer<Map> onProducerResponse =
                map -> {
                    final String status = (String) map.getOrDefault("status", "OK");
                    if (!"OK".equals(status)) {
                        err(String.format("Error sending message, got response: %s", map));
                    } else {
                        logUser("✅");
                    }
                    waitingProduceResponse.set(false);
                };

        final Consumer<Map> onConsumerMessage =
                map -> {
                    final Map<String, Object> record = (Map<String, Object>) map.get("record");
                    Map<String, String> headers = (Map<String, String>) record.get("headers");
                    boolean isLastMessage = false;
                    int streamIndex = -1;
                    if (headers != null) {
                        String streamLastMessage = headers.get("stream-last-message");
                        if (streamLastMessage != null) {
                            isStreamingOutput.set(true);
                            isLastMessage = Boolean.parseBoolean(streamLastMessage + "");
                            streamIndex =
                                    Integer.parseInt(headers.getOrDefault("stream-index", "-1"));
                        }
                    }
                    if (isStreamingOutput.get()) {
                        if (streamIndex == 1) {
                            logServer("Server:");
                        }
                        logNoNewline(String.valueOf(record.get("value")));
                        if (isLastMessage) {
                            logServer("\n");
                            waitingConsumeMessage.set(false);
                            isStreamingOutput.set(false);
                            logServer(".");
                        }
                    } else {
                        logServer("\n");
                        logServer("Server:");
                        log(String.valueOf(record.get("value")));
                        waitingConsumeMessage.set(false);
                    }
                };

        final Consumer<CloseReason> onClose =
                closeReason -> {
                    if (closeReason.getCloseCode() != CloseReason.CloseCodes.NORMAL_CLOSURE) {
                        err(
                                String.format(
                                        "Server closed connection with unexpected code: %s %s",
                                        closeReason.getCloseCode(), closeReason.getReasonPhrase()));
                    }
                    final CompletableFuture<Void> future = loop.get();
                    future.cancel(true);
                };
        try {
            connection.start(onConsumerMessage, onProducerResponse, onClose);

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
                                        connection.produce(
                                                messageMapper.writeValueAsString(
                                                        new ProduceGatewayCmd.ProduceRequest(
                                                                null, line, Map.of())));
                                        waitingProduceResponse.set(true);
                                        waitingConsumeMessage.set(true);
                                        isStreamingOutput.set(false);
                                        while (waitingProduceResponse.get()) {
                                            Thread.sleep(500);
                                            if (waitingProduceResponse.get()) {
                                                logUserNoNewLine(".");
                                            }
                                        }
                                        while (waitingConsumeMessage.get()) {
                                            Thread.sleep(500);
                                            if (waitingConsumeMessage.get()) {
                                                // If streaming, just flush the line, otherwise
                                                // print a dot
                                                if (isStreamingOutput.get()) {
                                                    logUserFlush();
                                                } else {
                                                    logUserNoNewLine(".");
                                                }
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
        } finally {
            connection.close();
        }
    }

    private GatewayConnection createGatewayConnection() {
        if (chatGatewayId != null && (produceToGatewayId != null || consumeFromGatewayId != null)) {
            throw new IllegalArgumentException(
                    "Cannot specify both chat gateway id and produce/--consume-from-gateway");
        }
        if (produceToGatewayId != null && consumeFromGatewayId == null) {
            throw new IllegalArgumentException(
                    "Cannot specify --produce-to-gateway without --consume-from-gateway");
        }
        if (consumeFromGatewayId != null && produceToGatewayId == null) {
            throw new IllegalArgumentException(
                    "Cannot specify --consume-from-gateway without --produce-to-gateway");
        }

        final Map<String, String> consumeGatewayOptions = Map.of("position", "latest");
        final Duration connectTimeout =
                connectTimeoutSeconds > 0 ? Duration.ofSeconds(connectTimeoutSeconds) : null;

        if (chatGatewayId != null) {
            final Map<String, String> finalParams = new HashMap<>();
            final Map<String, String> generatedParams =
                    generatedParamsForChatGateway(applicationId, chatGatewayId);
            if (generatedParams != null) {
                finalParams.putAll(generatedParams);
            }
            if (params != null) {
                finalParams.putAll(params);
            }
            final String url =
                    validateGatewayAndGetUrl(
                            applicationId,
                            chatGatewayId,
                            Gateways.Gateway.TYPE_CHAT,
                            finalParams,
                            consumeGatewayOptions,
                            credentials,
                            testCredentials,
                            Protocols.ws);
            return new ChatGatewayConnection(url, connectTimeout);
        }

        final String consumePath =
                validateGatewayAndGetUrl(
                        applicationId,
                        consumeFromGatewayId,
                        Gateways.Gateway.TYPE_CONSUME,
                        params,
                        consumeGatewayOptions,
                        credentials,
                        testCredentials,
                        Protocols.ws);
        final String producePath =
                validateGatewayAndGetUrl(
                        applicationId,
                        produceToGatewayId,
                        Gateways.Gateway.TYPE_PRODUCE,
                        params,
                        Map.of(),
                        credentials,
                        testCredentials,
                        Protocols.ws);
        return new ProduceConsumeGatewaysConnection(consumePath, producePath, connectTimeout);
    }

    private void logUser(String message) {
        log("\u001B[34m" + message + "\u001B[0m");
        command.commandLine().getOut().flush();
    }

    private void logUserNoNewLine(String message) {
        command.commandLine().getOut().print("\u001B[34m" + message + "\u001B[0m");
        command.commandLine().getOut().flush();
    }

    private void logUserFlush() {
        command.commandLine().getOut().flush();
    }

    private void logServer(String message) {
        log("\u001B[32m" + message + "\u001B[0m");
        command.commandLine().getOut().flush();
    }

    private class ProduceConsumeGatewaysConnection implements GatewayConnection {
        private final String consumePath;
        private final String producePath;
        private final Duration connectTimeout;

        private WebSocketClient consumeClient;
        private WebSocketClient produceClient;

        public ProduceConsumeGatewaysConnection(
                String consumePath, String producePath, Duration connectTimeout) {
            this.consumePath = consumePath;
            this.producePath = producePath;
            this.connectTimeout = connectTimeout;
        }

        @Override
        @SneakyThrows
        public void start(
                Consumer<Map> onConsumeMessage,
                Consumer<Map> onProducerResponse,
                Consumer<CloseReason> onClose) {
            CountDownLatch consumerReady = new CountDownLatch(1);
            consumeClient =
                    new WebSocketClient(
                                    new WebSocketClient.Handler() {

                                        @Override
                                        public void onOpen() {
                                            log(String.format("Connected to %s", consumePath));
                                            consumerReady.countDown();
                                        }

                                        @Override
                                        public void onMessage(String msg) {

                                            try {
                                                final Map response =
                                                        messageMapper.readValue(msg, Map.class);
                                                onConsumeMessage.accept(response);
                                            } catch (Throwable error) {
                                                err(
                                                        String.format(
                                                                "Error consuming message: %s %s",
                                                                msg, error.getMessage()));
                                            }
                                        }

                                        @Override
                                        public void onClose(CloseReason closeReason) {
                                            onClose.accept(closeReason);
                                        }

                                        @Override
                                        public void onError(Throwable throwable) {
                                            err(
                                                    String.format(
                                                            "Connection error: %s",
                                                            throwable.getMessage()));
                                        }
                                    })
                            .connect(URI.create(consumePath), connectTimeout);
            produceClient =
                    new WebSocketClient(
                                    new WebSocketClient.Handler() {
                                        @Override
                                        public void onOpen() {
                                            log(String.format("Connected to %s", producePath));
                                        }

                                        @Override
                                        public void onMessage(String msg) {
                                            try {
                                                final Map response =
                                                        messageMapper.readValue(msg, Map.class);
                                                onProducerResponse.accept(response);
                                            } catch (Throwable error) {
                                                err(
                                                        String.format(
                                                                "Error consuming producer response: %s %s",
                                                                msg, error.getMessage()));
                                            }
                                        }

                                        @Override
                                        public void onClose(CloseReason closeReason) {
                                            onClose.accept(closeReason);
                                        }

                                        @Override
                                        public void onError(Throwable throwable) {
                                            err(
                                                    String.format(
                                                            "Connection error: %s",
                                                            throwable.getMessage()));
                                        }
                                    })
                            .connect(URI.create(producePath), connectTimeout);
            consumerReady.await();
        }

        @Override
        public void produce(String message) {
            produceClient.send(message);
        }

        @Override
        @SneakyThrows
        public void close() {
            if (consumeClient != null) {
                consumeClient.close();
            }
            if (produceClient != null) {
                produceClient.close();
            }
        }
    }

    private interface GatewayConnection extends AutoCloseable {

        void start(
                Consumer<Map> onConsumeMessage,
                Consumer<Map> onProducerResponse,
                Consumer<CloseReason> onClose);

        void produce(String message);
    }

    private class ChatGatewayConnection implements GatewayConnection {
        private final String gatewayPath;
        private final Duration connectTimeout;

        private WebSocketClient client;

        public ChatGatewayConnection(String gatewayPath, Duration connectTimeout) {
            this.gatewayPath = gatewayPath;
            this.connectTimeout = connectTimeout;
        }

        @Override
        @SneakyThrows
        public void start(
                Consumer<Map> onConsumeMessage,
                Consumer<Map> onProducerResponse,
                Consumer<CloseReason> onClose) {
            CountDownLatch consumerReady = new CountDownLatch(1);
            client =
                    new WebSocketClient(
                                    new WebSocketClient.Handler() {

                                        @Override
                                        public void onOpen() {
                                            log(String.format("Connected to %s", gatewayPath));
                                            consumerReady.countDown();
                                        }

                                        @Override
                                        public void onMessage(String msg) {
                                            try {
                                                final Map response =
                                                        messageMapper.readValue(msg, Map.class);
                                                if (response.containsKey("status")) {
                                                    onProducerResponse.accept(response);
                                                } else {
                                                    onConsumeMessage.accept(response);
                                                }
                                            } catch (Throwable error) {
                                                err(
                                                        String.format(
                                                                "Error consuming message: %s %s",
                                                                msg, error.getMessage()));
                                            }
                                        }

                                        @Override
                                        public void onClose(CloseReason closeReason) {
                                            onClose.accept(closeReason);
                                        }

                                        @Override
                                        public void onError(Throwable throwable) {
                                            err(
                                                    String.format(
                                                            "Connection error: %s",
                                                            throwable.getMessage()));
                                        }
                                    })
                            .connect(URI.create(gatewayPath), connectTimeout);
            consumerReady.await();
        }

        @Override
        public void produce(String message) {
            client.send(message);
        }

        @Override
        @SneakyThrows
        public void close() {
            if (client != null) {
                client.close();
            }
        }
    }
}
