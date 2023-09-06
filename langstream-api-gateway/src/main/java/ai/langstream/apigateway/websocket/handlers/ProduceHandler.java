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
package ai.langstream.apigateway.websocket.handlers;

import static ai.langstream.apigateway.websocket.WebSocketConfig.PRODUCE_PATH;

import ai.langstream.api.model.Gateway;
import ai.langstream.api.model.StreamingCluster;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.SimpleRecord;
import ai.langstream.api.runner.topics.TopicConnectionsRuntime;
import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.api.storage.ApplicationStore;
import ai.langstream.apigateway.websocket.AuthenticatedGatewayRequestContext;
import ai.langstream.apigateway.websocket.api.ProduceRequest;
import ai.langstream.apigateway.websocket.api.ProduceResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

@Slf4j
public class ProduceHandler extends AbstractHandler {

    public ProduceHandler(ApplicationStore applicationStore) {
        super(applicationStore);
    }

    @Override
    public String path() {
        return PRODUCE_PATH;
    }

    @Override
    Gateway.GatewayType gatewayType() {
        return Gateway.GatewayType.produce;
    }

    @Override
    String tenantFromPath(Map<String, String> parsedPath, Map<String, String> queryString) {
        return parsedPath.get("tenant");
    }

    @Override
    String applicationIdFromPath(Map<String, String> parsedPath, Map<String, String> queryString) {
        return parsedPath.get("application");
    }

    @Override
    String gatewayFromPath(Map<String, String> parsedPath, Map<String, String> queryString) {
        return parsedPath.get("gateway");
    }

    @Override
    public void onBeforeHandshakeCompleted(
            AuthenticatedGatewayRequestContext context, Map<String, Object> attributes)
            throws Exception {
        Gateway gateway = context.gateway();

        final List<Header> headers =
                getCommonHeaders(gateway, context.userParameters(), context.principalValues());
        final StreamingCluster streamingCluster =
                context.application().getInstance().streamingCluster();

        final TopicConnectionsRuntime topicConnectionsRuntime =
                TOPIC_CONNECTIONS_REGISTRY.getTopicConnectionsRuntime(streamingCluster);

        final String topicName = gateway.topic();
        final TopicProducer producer =
                topicConnectionsRuntime.createProducer(
                        null, streamingCluster, Map.of("topic", topicName));
        recordCloseableResource(attributes, producer);
        producer.start();

        attributes.put("producer", producer);
        attributes.put("headers", Collections.unmodifiableList(headers));
        log.info(
                "Started produced for gateway {}/{}/{} on topic {}",
                context.tenant(),
                context.applicationId(),
                context.gateway().id(),
                topicName);
        sendClientConnectedEvent(context);
    }

    @Override
    public void onOpen(
            WebSocketSession webSocketSession, AuthenticatedGatewayRequestContext context) {}

    @Override
    public void onMessage(
            WebSocketSession webSocketSession,
            AuthenticatedGatewayRequestContext context,
            TextMessage message)
            throws Exception {
        final TopicProducer topicProducer = getTopicProducer(webSocketSession, true);
        final ProduceRequest produceRequest;
        try {
            produceRequest = mapper.readValue(message.getPayload(), ProduceRequest.class);
        } catch (JsonProcessingException err) {
            sendResponse(webSocketSession, ProduceResponse.Status.BAD_REQUEST, err.getMessage());
            return;
        }
        if (produceRequest.value() == null && produceRequest.key() == null) {
            sendResponse(
                    webSocketSession,
                    ProduceResponse.Status.BAD_REQUEST,
                    "Either key or value must be set.");
            return;
        }

        final Collection<Header> headers =
                new ArrayList<>((List<Header>) webSocketSession.getAttributes().get("headers"));
        if (produceRequest.headers() != null) {
            final Set<String> configuredHeaders =
                    headers.stream().map(Header::key).collect(Collectors.toSet());
            log.info(
                    "configuredHeaders: {} passed {}", configuredHeaders, produceRequest.headers());
            for (Map.Entry<String, String> messageHeader : produceRequest.headers().entrySet()) {
                if (configuredHeaders.contains(messageHeader.getKey())) {
                    sendResponse(
                            webSocketSession,
                            ProduceResponse.Status.BAD_REQUEST,
                            "Header "
                                    + messageHeader.getKey()
                                    + " is configured as parameter-level header.");
                    return;
                }
                headers.add(
                        SimpleRecord.SimpleHeader.of(
                                messageHeader.getKey(), messageHeader.getValue()));
            }
        }
        try {
            final SimpleRecord record = SimpleRecord
                    .builder()
                    .key(produceRequest.key())
                    .value(produceRequest.value())
                    .headers(headers)
                    .build();
            topicProducer.write(record).get();
            log.info("[{}] Produced record {}", webSocketSession.getId(), record);
        } catch (Throwable tt) {
            sendResponse(webSocketSession, ProduceResponse.Status.PRODUCER_ERROR, tt.getMessage());
            return;
        }

        webSocketSession.sendMessage(
                new TextMessage(mapper.writeValueAsString(ProduceResponse.OK)));
    }

    private void sendResponse(
            WebSocketSession webSocketSession, ProduceResponse.Status status, String reason)
            throws IOException {
        webSocketSession.sendMessage(
                new TextMessage(mapper.writeValueAsString(new ProduceResponse(status, reason))));
    }

    private TopicProducer getTopicProducer(
            WebSocketSession webSocketSession, boolean throwIfNotFound) {
        final TopicProducer topicProducer =
                (TopicProducer) webSocketSession.getAttributes().get("producer");
        if (topicProducer == null) {
            if (throwIfNotFound) {
                log.error("No producer found for session {}", webSocketSession.getId());
                throw new IllegalStateException(
                        "No producer found for session " + webSocketSession.getId());
            }
        }
        return topicProducer;
    }

    @Override
    public void onClose(
            WebSocketSession webSocketSession,
            AuthenticatedGatewayRequestContext context,
            CloseStatus status) {}

    @Override
    void validateOptions(Map<String, String> options) {
        for (Map.Entry<String, String> option : options.entrySet()) {
            switch (option.getKey()) {
                default -> throw new IllegalArgumentException("Unknown option " + option.getKey());
            }
        }
    }

    private List<Header> getCommonHeaders(
            Gateway selectedGateway,
            Map<String, String> passedParameters,
            Map<String, String> principalValues) {
        final List<Header> headers = new ArrayList<>();
        if (selectedGateway.produceOptions() != null
                && selectedGateway.produceOptions().headers() != null) {
            final List<Gateway.KeyValueComparison> headersConfig =
                    selectedGateway.produceOptions().headers();
            for (Gateway.KeyValueComparison mapping : headersConfig) {
                if (mapping.key() == null || mapping.key().isEmpty()) {
                    throw new IllegalArgumentException("Header key cannot be empty");
                }
                String value = mapping.value();
                if (value == null && mapping.valueFromParameters() != null) {
                    value = passedParameters.get(mapping.valueFromParameters());
                }
                if (value == null && mapping.valueFromAuthentication() != null) {
                    value = principalValues.get(mapping.valueFromAuthentication());
                }
                if (value == null) {
                    throw new IllegalArgumentException(mapping.key() + "header cannot be empty");
                }

                headers.add(SimpleRecord.SimpleHeader.of(mapping.key(), value));
            }
        }
        return headers;
    }
}
