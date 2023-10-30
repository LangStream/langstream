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
package ai.langstream.apigateway.gateways;

import ai.langstream.api.model.Gateway;
import ai.langstream.api.model.StreamingCluster;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.SimpleRecord;
import ai.langstream.api.runner.topics.TopicConnectionsRuntime;
import ai.langstream.api.runner.topics.TopicConnectionsRuntimeRegistry;
import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.apigateway.api.ProduceRequest;
import ai.langstream.apigateway.api.ProduceResponse;
import ai.langstream.apigateway.websocket.AuthenticatedGatewayRequestContext;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProduceGateway implements AutoCloseable {

    protected static final ObjectMapper mapper = new ObjectMapper();

    @Getter
    public static class ProduceException extends Exception {

        private final ProduceResponse.Status status;

        public ProduceException(String message, ProduceResponse.Status status) {
            super(message);
            this.status = status;
        }

        public ProduceException(String message, ProduceResponse.Status status, Throwable tt) {
            super(message, tt);
            this.status = status;
        }
    }

    public static class ProduceGatewayRequestValidator
            implements GatewayRequestHandler.GatewayRequestValidator {
        @Override
        public List<String> getAllRequiredParameters(Gateway gateway) {
            return gateway.getParameters();
        }

        @Override
        public void validateOptions(Map<String, String> options) {
            for (Map.Entry<String, String> option : options.entrySet()) {
                switch (option.getKey()) {
                    default -> throw new IllegalArgumentException(
                            "Unknown option " + option.getKey());
                }
            }
        }
    }

    private final TopicConnectionsRuntimeRegistry topicConnectionsRuntimeRegistry;
    private TopicProducer producer;
    private List<Header> commonHeaders;
    private String logRef;

    public ProduceGateway(TopicConnectionsRuntimeRegistry topicConnectionsRuntimeRegistry) {
        this.topicConnectionsRuntimeRegistry = topicConnectionsRuntimeRegistry;
    }

    public void start(
            String topic,
            List<Header> commonHeaders,
            AuthenticatedGatewayRequestContext requestContext) {
        this.logRef =
                "%s/%s/%s"
                        .formatted(
                                requestContext.tenant(),
                                requestContext.applicationId(),
                                requestContext.gateway().getId());
        this.commonHeaders = commonHeaders == null ? List.of() : commonHeaders;

        setupProducer(
                topic,
                requestContext.application().getInstance().streamingCluster(),
                requestContext.tenant(),
                requestContext.applicationId(),
                requestContext.gateway().getId());
    }

    protected void setupProducer(
            String topic,
            StreamingCluster streamingCluster,
            final String tenant,
            final String applicationId,
            final String gatewayId) {
        final TopicConnectionsRuntime topicConnectionsRuntime =
                topicConnectionsRuntimeRegistry
                        .getTopicConnectionsRuntime(streamingCluster)
                        .asTopicConnectionsRuntime();

        topicConnectionsRuntime.init(streamingCluster);

        producer =
                topicConnectionsRuntime.createProducer(
                        null, streamingCluster, Map.of("topic", topic));
        producer.start();
        log.debug("[{}] Started producer on topic {}", logRef, topic);
    }

    public void produceMessage(String payload) throws ProduceException {
        final ProduceRequest produceRequest;
        try {
            produceRequest = mapper.readValue(payload, ProduceRequest.class);
        } catch (JsonProcessingException err) {
            throw new ProduceException(err.getMessage(), ProduceResponse.Status.BAD_REQUEST);
        }
        produceMessage(produceRequest);
    }

    public void produceMessage(ProduceRequest produceRequest) throws ProduceException {
        if (produceRequest.value() == null && produceRequest.key() == null) {
            throw new ProduceException(
                    "Either key or value must be set.", ProduceResponse.Status.BAD_REQUEST);
        }
        if (producer == null) {
            throw new ProduceException(
                    "Producer not initialized", ProduceResponse.Status.PRODUCER_ERROR);
        }

        final Collection<Header> headers = new ArrayList<>(commonHeaders);
        if (produceRequest.headers() != null) {
            final Set<String> configuredHeaders =
                    headers.stream().map(Header::key).collect(Collectors.toSet());
            for (Map.Entry<String, String> messageHeader : produceRequest.headers().entrySet()) {
                if (configuredHeaders.contains(messageHeader.getKey())) {
                    throw new ProduceException(
                            "Header "
                                    + messageHeader.getKey()
                                    + " is configured as parameter-level header.",
                            ProduceResponse.Status.BAD_REQUEST);
                }
                headers.add(
                        SimpleRecord.SimpleHeader.of(
                                messageHeader.getKey(), messageHeader.getValue()));
            }
        }
        try {
            final SimpleRecord record =
                    SimpleRecord.builder()
                            .key(produceRequest.key())
                            .value(produceRequest.value())
                            .headers(headers)
                            .build();
            producer.write(record).get();
            log.debug("[{}] Produced record {}", logRef, record);
        } catch (Throwable tt) {
            log.error("[{}] Error producing message: {}", logRef, tt.getMessage(), tt);
            throw new ProduceException(tt.getMessage(), ProduceResponse.Status.PRODUCER_ERROR, tt);
        }
    }

    @Override
    public void close() {
        if (producer != null) {
            try {
                producer.close();
            } catch (Exception e) {
                log.debug("[{}] Error closing producer: {}", logRef, e.getMessage(), e);
            }
        }
    }

    public static List<Header> getProducerCommonHeaders(
            Gateway.ProduceOptions produceOptions, AuthenticatedGatewayRequestContext context) {
        if (produceOptions == null) {
            return null;
        }
        return getProducerCommonHeaders(
                produceOptions.headers(), context.userParameters(), context.principalValues());
    }

    public static List<Header> getProducerCommonHeaders(
            Gateway.ChatOptions chatOptions, AuthenticatedGatewayRequestContext context) {
        if (chatOptions == null) {
            return null;
        }
        return getProducerCommonHeaders(
                chatOptions.getHeaders(), context.userParameters(), context.principalValues());
    }

    public static List<Header> getProducerCommonHeaders(
            Gateway.ServiceOptions serviceOptions, AuthenticatedGatewayRequestContext context) {
        if (serviceOptions == null) {
            return null;
        }
        return getProducerCommonHeaders(
                serviceOptions.getHeaders(), context.userParameters(), context.principalValues());
    }

    public static List<Header> getProducerCommonHeaders(
            List<Gateway.KeyValueComparison> headerFilters,
            Map<String, String> passedParameters,
            Map<String, String> principalValues) {
        final List<Header> headers = new ArrayList<>();
        if (headerFilters == null) {
            return headers;
        }
        for (Gateway.KeyValueComparison mapping : headerFilters) {
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
                throw new IllegalArgumentException("header " + mapping.key() + " cannot be empty");
            }
            headers.add(SimpleRecord.SimpleHeader.of(mapping.key(), value));
        }
        return headers;
    }
}
