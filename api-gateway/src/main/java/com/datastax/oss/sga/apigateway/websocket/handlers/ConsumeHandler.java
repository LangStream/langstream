package com.datastax.oss.sga.apigateway.websocket.handlers;

import static com.datastax.oss.sga.apigateway.websocket.WebSocketConfig.CONSUME_PATH;
import com.datastax.oss.sga.api.model.Gateway;
import com.datastax.oss.sga.api.model.Gateways;
import com.datastax.oss.sga.api.model.StoredApplication;
import com.datastax.oss.sga.api.model.StreamingCluster;
import com.datastax.oss.sga.api.model.TopicDefinition;
import com.datastax.oss.sga.api.runner.code.Header;
import com.datastax.oss.sga.api.runner.code.Record;
import com.datastax.oss.sga.api.runner.topics.TopicConnectionsRuntime;
import com.datastax.oss.sga.api.runner.topics.TopicConsumer;
import com.datastax.oss.sga.api.storage.ApplicationStore;
import com.datastax.oss.sga.apigateway.websocket.api.ConsumePushMessage;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.logging.Filter;
import lombok.extern.slf4j.Slf4j;
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
        final TopicConsumer consumer = (TopicConsumer) webSocketSession.getAttributes().get("consumer");
        if (consumer != null) {
            consumer.close();
        }
    }

    private void setupConsumer(WebSocketSession session, String gatewayId, String tenant, String applicationId)
            throws Exception {
        final StoredApplication application = applicationStore.get(tenant, applicationId);
        Gateway selectedGateway = extractGateway(gatewayId, application, Gateway.GatewayType.consume);

        final Map<String, String> passedParameters = verifyParameters(session, selectedGateway);

        List<Function<Record, Boolean>> filters =
                createMessageFilters(selectedGateway, passedParameters);

        final StreamingCluster streamingCluster = application.getInstance().getInstance().streamingCluster();

        final TopicConnectionsRuntime topicConnectionsRuntime =
                TOPIC_CONNECTIONS_REGISTRY.getTopicConnectionsRuntime(streamingCluster);

        final String topicName = selectedGateway.topic();
        final TopicConsumer consumer =
                topicConnectionsRuntime.createConsumer("ag-" + session.getId(), streamingCluster,
                        Map.of("topic", topicName));
        consumer.start();
        log.info("[{}] Started consumer for gateway {}/{}/{}", session.getId(), tenant, applicationId, gatewayId);
        session.getAttributes().put("consumer", consumer);


        while (true) {
            final List<Record> records = consumer.read();
            for (Record record : records) {
                boolean skip = false;
                for (Function<Record, Boolean> filter : filters) {
                    if (!filter.apply(record)) {
                        skip = true;
                        break;
                    }
                }
                if (!skip) {
                    final Collection<Header> headers = record.headers();
                    final Map<String, String> messageHeaders;
                    if (headers == null) {
                        messageHeaders = Map.of();
                    } else {
                        messageHeaders = new HashMap<>();
                        headers.stream().forEach(h -> messageHeaders.put(h.key(), h.valueAsString()));
                    }
                    final String message = mapper.writeValueAsString(new ConsumePushMessage(
                            new ConsumePushMessage.Record(record.key(), record.value(), messageHeaders)));
                    session.sendMessage(new TextMessage(message));
                }
            }
        }
    }

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
