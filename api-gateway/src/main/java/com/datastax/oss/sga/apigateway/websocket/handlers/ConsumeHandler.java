package com.datastax.oss.sga.apigateway.websocket.handlers;

import static com.datastax.oss.sga.apigateway.websocket.WebSocketConfig.CONSUME_PATH;
import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.Gateway;
import com.datastax.oss.sga.api.model.StoredApplication;
import com.datastax.oss.sga.api.model.StreamingCluster;
import com.datastax.oss.sga.api.runner.code.Header;
import com.datastax.oss.sga.api.runner.code.Record;
import com.datastax.oss.sga.api.runner.topics.OffsetPerPartition;
import com.datastax.oss.sga.api.runner.topics.TopicConnectionsRuntime;
import com.datastax.oss.sga.api.runner.topics.TopicReadResult;
import com.datastax.oss.sga.api.runner.topics.TopicReader;
import com.datastax.oss.sga.api.runner.topics.TopicOffsetPosition;
import com.datastax.oss.sga.api.storage.ApplicationStore;
import com.datastax.oss.sga.apigateway.websocket.api.ConsumePushMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

@Slf4j
public class ConsumeHandler extends AbstractHandler {

    public ConsumeHandler(ApplicationStore applicationStore) {
        super(applicationStore);
    }

    @Override
    public String path() {
        return CONSUME_PATH;
    }

    @Override
    public void onBeforeHandshakeCompleted(Map<String, Object> attributes) throws Exception {
        final String tenant = (String) attributes.get("tenant");
        final String gatewayId = (String) attributes.get("gateway");
        final String applicationId = (String) attributes.get("application");
        final Application application = getResolvedApplication(tenant, applicationId);
        Gateway selectedGateway = extractGateway(gatewayId, application, Gateway.GatewayType.consume);


        final RequestDetails requestOptions = validateQueryStringAndOptions(
                (Map<String, String>) attributes.get("queryString"), selectedGateway);
        List<Function<Record, Boolean>> filters =
                createMessageFilters(selectedGateway, requestOptions.getUserParameters());
        attributes.put("consumeFilters", filters);

        final StreamingCluster streamingCluster = application.getInstance().streamingCluster();

        final TopicConnectionsRuntime topicConnectionsRuntime =
                TOPIC_CONNECTIONS_REGISTRY.getTopicConnectionsRuntime(streamingCluster);

        final String topicName = selectedGateway.topic();

        final String positionParameter = requestOptions.getOptions().getOrDefault("position", "latest");
        TopicOffsetPosition position = switch (positionParameter) {
            case "latest" -> TopicOffsetPosition.LATEST;
            case "earliest" -> TopicOffsetPosition.EARLIEST;
            default -> TopicOffsetPosition.absolute(new String(
                    Base64.getDecoder().decode(positionParameter), StandardCharsets.UTF_8));
        };
        TopicReader reader =
                topicConnectionsRuntime.createReader(streamingCluster,
                        Map.of("topic", topicName), position);
        reader.start();
        attributes.put("topicReader", reader);
    }

    @Override
    public void onOpen(WebSocketSession session) throws Exception {
        // we must return the caller thread to the thread pool
        final CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            final Map<String, Object> attributes = session.getAttributes();
            TopicReader reader = (TopicReader) attributes.get("topicReader");
            final String tenant = (String) attributes.get("tenant");
            final String gatewayId = (String) attributes.get("gateway");
            final String applicationId = (String) attributes.get("application");
            try {
                log.info("[{}] Started reader for gateway {}/{}/{}", session.getId(), tenant, applicationId, gatewayId);
                readMessages(session, (List<Function<Record, Boolean>>) attributes.get("consumeFilters"), reader);
            } catch (InterruptedException | CancellationException ex) {
                // ignore
            } catch (Throwable ex) {
                log.error(ex.getMessage(), ex);
            } finally {
                closeReader(reader);
            }
        });
        session.getAttributes().put("future", future);
    }

    @Override
    public void onMessage(WebSocketSession webSocketSession, TextMessage message) throws Exception {
    }

    @Override
    public void onClose(WebSocketSession webSocketSession, CloseStatus closeStatus) throws Exception {
        final CompletableFuture<Void> future = (CompletableFuture<Void>) webSocketSession.getAttributes().get("future");
        if (future != null && !future.isDone()) {
            future.cancel(true);
        }
    }

    @Override
    void validateOptions(Map<String, String> options) {
        for (Map.Entry<String, String> option : options.entrySet()) {
            switch (option.getKey()) {
                case "position":
                    if (StringUtils.isBlank(option.getValue())) {
                        throw new IllegalArgumentException("'position' cannot be blank");
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Unknown option " + option.getKey());
            }
        }
    }

    private void closeReader(TopicReader reader) {
        if (reader == null) {
            return;
        }
        try {
            reader.close();
        } catch (Exception e) {
            log.error("error closing reader", e);
        }
    }


    private void readMessages(WebSocketSession session, List<Function<Record, Boolean>> filters, TopicReader reader)
            throws Exception {
        while (true) {
            if (Thread.currentThread().isInterrupted()) {
                return;
            }
            if (!session.isOpen()) {
                return;
            }
            final TopicReadResult readResult = reader.read();
            final List<Record> records = readResult.records();
            for (Record record : records) {
                log.debug("[{}] Received record {}", session.getId(), record);
                boolean skip = false;
                for (Function<Record, Boolean> filter : filters) {
                    if (!filter.apply(record)) {
                        skip = true;
                        log.debug("[{}] Skipping record {}", session.getId(), record);
                        break;
                    }
                }
                if (!skip) {
                    final Map<String, String> messageHeaders = computeMessageHeaders(record);
                    final String offset = computeOffset(readResult);

                    final ConsumePushMessage message = new ConsumePushMessage(
                            new ConsumePushMessage.Record(record.key(), record.value(), messageHeaders),
                            offset
                    );
                    final String jsonMessage = mapper.writeValueAsString(message);
                    session.sendMessage(new TextMessage(jsonMessage));
                }
            }
        }
    }

    private Map<String, String> computeMessageHeaders(Record record) {
        final Collection<Header> headers = record.headers();
        final Map<String, String> messageHeaders;
        if (headers == null) {
            messageHeaders = Map.of();
        } else {
            messageHeaders = new HashMap<>();
            headers.stream().forEach(h -> messageHeaders.put(h.key(), h.valueAsString()));
        }
        return messageHeaders;
    }

    private String computeOffset(TopicReadResult readResult) throws JsonProcessingException {
        final OffsetPerPartition offsetPerPartition = readResult.partitionsOffsets();
        if (offsetPerPartition == null) {
            return null;
        }
        final String offsets = Base64.getEncoder().encodeToString(mapper.writeValueAsBytes(offsetPerPartition));
        return offsets;
    }

    public record Offset(String partition, String offset) {}

    private List<Function<Record, Boolean>> createMessageFilters(Gateway selectedGateway,
                                                                 Map<String, String> passedParameters) {
        List<Function<Record, Boolean>> filters = new ArrayList<>();

        if (selectedGateway.consumeOptions() != null) {
            final Gateway.ConsumeOptions consumeOptions = selectedGateway.consumeOptions();
            if (consumeOptions.filters() != null) {
                if (consumeOptions.filters().headers() != null) {
                    for (Gateway.KeyValueComparison comparison : consumeOptions.filters().headers()) {
                        if (comparison.key() == null) {
                            throw new IllegalArgumentException("Key cannot be null");
                        }
                        filters.add(new Function<Record, Boolean>() {
                            @Override
                            public Boolean apply(Record record) {
                                final Header header = record.getHeader(comparison.key());
                                if (header == null) {
                                    return false;
                                }
                                final String expectedValue = header.valueAsString();
                                if (expectedValue == null) {
                                    return false;
                                }
                                String value = comparison.value();
                                if (value == null && comparison.valueFromParameters() != null) {
                                    value = passedParameters.get(comparison.valueFromParameters());
                                }
                                if (value == null) {
                                    return false;
                                }
                                return expectedValue.equals(value);
                            }
                        });
                    }

                }
            }


        }
        return filters;
    }
}
