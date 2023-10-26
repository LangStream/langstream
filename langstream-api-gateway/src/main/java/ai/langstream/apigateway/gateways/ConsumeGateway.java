package ai.langstream.apigateway.gateways;

import ai.langstream.api.model.Gateway;
import ai.langstream.api.model.StreamingCluster;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.topics.TopicConnectionsRuntime;
import ai.langstream.api.runner.topics.TopicConnectionsRuntimeRegistry;
import ai.langstream.api.runner.topics.TopicOffsetPosition;
import ai.langstream.api.runner.topics.TopicReadResult;
import ai.langstream.api.runner.topics.TopicReader;
import ai.langstream.apigateway.websocket.AuthenticatedGatewayRequestContext;
import ai.langstream.apigateway.websocket.api.ConsumePushMessage;
import ai.langstream.apigateway.websocket.api.ProduceResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Closeable;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConsumeGateway implements Closeable {

    protected static final ObjectMapper mapper = new ObjectMapper();

    @Getter
    public static class ProduceException extends Exception {

        private final ProduceResponse.Status status;

        public ProduceException(String message, ProduceResponse.Status status) {
            super(message);
            this.status = status;
        }
    }

    public static class ProduceGatewayRequestValidator implements GatewayRequestHandler.GatewayRequestValidator {
        @Override
        public List<String> getAllRequiredParameters(Gateway gateway) {
            return gateway.getParameters();
        }

        @Override
        public void validateOptions(Map<String, String> options) {
            for (Map.Entry<String, String> option : options.entrySet()) {
                switch (option.getKey()) {
                    default -> throw new IllegalArgumentException("Unknown option " + option.getKey());
                }
            }
        }
    }


    private final TopicConnectionsRuntimeRegistry topicConnectionsRuntimeRegistry;
    private TopicReader reader;
    private AuthenticatedGatewayRequestContext requestContext;
    private List<Function<Record, Boolean>> filters;

    public ConsumeGateway(TopicConnectionsRuntimeRegistry topicConnectionsRuntimeRegistry) {
        this.topicConnectionsRuntimeRegistry = topicConnectionsRuntimeRegistry;
    }

    @SneakyThrows
    public void setup(String topic, List<Function<Record, Boolean>> filters, AuthenticatedGatewayRequestContext requestContext) {
        this.requestContext = requestContext;
        this.filters = filters == null ? List.of() : filters;

        final StreamingCluster streamingCluster = requestContext.application().getInstance().streamingCluster();
        final TopicConnectionsRuntime topicConnectionsRuntime =
                topicConnectionsRuntimeRegistry
                        .getTopicConnectionsRuntime(streamingCluster)
                        .asTopicConnectionsRuntime();

        topicConnectionsRuntime.init(streamingCluster);

        final String positionParameter = requestContext.options().getOrDefault("position", "latest");
        TopicOffsetPosition position =
                switch (positionParameter) {
                    case "latest" -> TopicOffsetPosition.LATEST;
                    case "earliest" -> TopicOffsetPosition.EARLIEST;
                    default -> TopicOffsetPosition.absolute(
                            Base64.getDecoder().decode(positionParameter));
                };
        reader =
                topicConnectionsRuntime.createReader(
                        streamingCluster, Map.of("topic", topic), position);
        reader.start();
    }


    public CompletableFuture<Void> startReading(Executor executor, Supplier<Boolean> stop, Consumer<String> onMessage) {
        if (requestContext == null || reader == null) {
            throw new IllegalStateException("Not initialized");
        }
        return CompletableFuture.runAsync(
                        () -> {

                            try {
                                final String tenant = requestContext.tenant();

                                final String gatewayId = requestContext.gateway().getId();
                                final String applicationId = requestContext.applicationId();
                                log.info(
                                        "Started reader for gateway {}/{}/{}",
                                        tenant,
                                        applicationId,
                                        gatewayId);
                                readMessages(stop, onMessage);
                            } catch (InterruptedException | CancellationException ex) {
                                // ignore
                            } catch (Throwable ex) {
                                log.error(ex.getMessage(), ex);
                            } finally {
                                closeReader();
                            }
                        },
                        executor);
    }


    protected void readMessages(Supplier<Boolean> stop, Consumer<String> onMessage)
            throws Exception {
        while (true) {
            if (Thread.currentThread().isInterrupted()) {
                return;
            }
            if (stop.get()) {
                return;
            }
            final TopicReadResult readResult = reader.read();
            final List<Record> records = readResult.records();
            for (Record record : records) {
                log.debug("Received record {}", record);
                boolean skip = false;
                if (filters != null) {
                    for (Function<Record, Boolean> filter : filters) {
                        if (!filter.apply(record)) {
                            skip = true;
                            log.debug("Skipping record {}", requestContext, record);
                            break;
                        }
                    }
                }
                if (!skip) {
                    final Map<String, String> messageHeaders = computeMessageHeaders(record);
                    final String offset = computeOffset(readResult);

                    final ConsumePushMessage message =
                            new ConsumePushMessage(
                                    new ConsumePushMessage.Record(
                                            record.key(), record.value(), messageHeaders),
                                    offset);
                    final String jsonMessage = mapper.writeValueAsString(message);
                    onMessage.accept(jsonMessage);

                }
            }
        }
    }




    private static Map<String, String> computeMessageHeaders(Record record) {
        final Collection<Header> headers = record.headers();
        final Map<String, String> messageHeaders;
        if (headers == null) {
            messageHeaders = Map.of();
        } else {
            messageHeaders = new HashMap<>();
            headers.forEach(h -> messageHeaders.put(h.key(), h.valueAsString()));
        }
        return messageHeaders;
    }

    private static String computeOffset(TopicReadResult readResult) {
        final byte[] offset = readResult.offset();
        if (offset == null) {
            return null;
        }
        return Base64.getEncoder().encodeToString(offset);
    }




    private void closeReader() {
        if (reader != null) {
            try {
                reader.close();
            } catch (Exception e) {
                log.error("error closing reader", e);
            }
        }
    }



    @Override
    public void close()  {
        closeReader();
    }

}
