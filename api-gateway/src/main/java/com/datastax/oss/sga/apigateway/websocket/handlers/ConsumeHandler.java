package com.datastax.oss.sga.apigateway.websocket.handlers;

import static com.datastax.oss.sga.apigateway.websocket.WebSocketConfig.CONSUME_PATH;
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
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

@Slf4j
public class ConsumeHandler extends AbstractHandler {

    public ConsumeHandler(ApplicationStore applicationStore) {
        super(applicationStore);
    }

    @Override
    public void onOpen(WebSocketSession webSocketSession) throws Exception {
        final String tenant = (String) webSocketSession.getAttributes().get("tenant");

        final AntPathMatcher antPathMatcher = new AntPathMatcher();
        final Map<String, String> vars =
                antPathMatcher.extractUriTemplateVariables(CONSUME_PATH,
                        webSocketSession.getUri().getPath());
        final String gateway = vars.get("gateway");
        final String applicationId = vars.get("application");
        setupConsumer(webSocketSession, gateway, tenant, applicationId);
    }

    @Override
    public void onMessage(WebSocketSession webSocketSession, TextMessage message) throws Exception {
    }

    @Override
    public void onClose(WebSocketSession webSocketSession, CloseStatus closeStatus) throws Exception {
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

    private void setupConsumer(WebSocketSession session, String gatewayId, String tenant, String applicationId)
            throws Exception {
        final StoredApplication application = applicationStore.get(tenant, applicationId);
        Gateway selectedGateway = extractGateway(gatewayId, application, Gateway.GatewayType.consume);

        final RequestDetails requestOptions = validateQueryStringAndOptions(session, selectedGateway);
        List<Function<Record, Boolean>> filters =
                createMessageFilters(selectedGateway, requestOptions.getUserParameters());

        final StreamingCluster streamingCluster = application.getInstance().getInstance().streamingCluster();

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

        final TopicReader reader =
                topicConnectionsRuntime.createReader(streamingCluster,
                        Map.of("topic", topicName), position);
        recordCloseableResource(session, reader);
        reader.start();
        log.info("[{}] Started reader for gateway {}/{}/{}", session.getId(), tenant, applicationId, gatewayId);

        while (true) {
            final boolean closed = Boolean.parseBoolean(session.getAttributes().getOrDefault("closed", false) + "");
            if (!session.isOpen() || closed) {
                break;
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
