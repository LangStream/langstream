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
package ai.langstream.apigateway.http;

import ai.langstream.api.gateway.GatewayRequestContext;
import ai.langstream.api.model.Gateway;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runtime.ClusterRuntimeRegistry;
import ai.langstream.api.storage.ApplicationStore;
import ai.langstream.apigateway.api.ProduceRequest;
import ai.langstream.apigateway.api.ProduceResponse;
import ai.langstream.apigateway.gateways.ConsumeGateway;
import ai.langstream.apigateway.gateways.GatewayRequestHandler;
import ai.langstream.apigateway.gateways.ProduceGateway;
import ai.langstream.apigateway.gateways.TopicProducerCache;
import ai.langstream.apigateway.runner.TopicConnectionsRuntimeProviderBean;
import ai.langstream.apigateway.websocket.AuthenticatedGatewayRequestContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.constraints.NotBlank;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.util.UriComponentsBuilder;

@RestController
@RequestMapping("/api/gateways")
@Slf4j
@AllArgsConstructor
public class GatewayResource {

    protected static final String GATEWAY_SERVICE_PATH =
            "/service/{tenant}/{application}/{gateway}/**";
    protected static final ObjectMapper MAPPER = new ObjectMapper();
    protected static final String SERVICE_REQUEST_ID_HEADER = "langstream-service-request-id";
    private final TopicConnectionsRuntimeProviderBean topicConnectionsRuntimeRegistryProvider;
    private final ClusterRuntimeRegistry clusterRuntimeRegistry;
    private final TopicProducerCache topicProducerCache;
    private final ApplicationStore applicationStore;
    private final GatewayRequestHandler gatewayRequestHandler;
    private final ExecutorService httpClientThreadPool =
            Executors.newCachedThreadPool(
                    new BasicThreadFactory.Builder().namingPattern("http-client-%d").build());
    private final HttpClient httpClient =
            HttpClient.newBuilder().executor(httpClientThreadPool).build();
    private final ExecutorService consumeThreadPool =
            Executors.newCachedThreadPool(
                    new BasicThreadFactory.Builder().namingPattern("http-consume-%d").build());

    @PostMapping(value = "/produce/{tenant}/{application}/{gateway}", consumes = "*/*")
    ProduceResponse produce(
            WebRequest request,
            @NotBlank @PathVariable("tenant") String tenant,
            @NotBlank @PathVariable("application") String application,
            @NotBlank @PathVariable("gateway") String gateway,
            @RequestBody String payload)
            throws ProduceGateway.ProduceException {

        final Map<String, String> queryString = computeQueryString(request);
        final Map<String, String> headers = computeHeaders(request);
        final GatewayRequestContext context =
                gatewayRequestHandler.validateRequest(
                        tenant,
                        application,
                        gateway,
                        Gateway.GatewayType.produce,
                        queryString,
                        headers,
                        new ProduceGateway.ProduceGatewayRequestValidator());
        final AuthenticatedGatewayRequestContext authContext;
        try {
            authContext = gatewayRequestHandler.authenticate(context);
        } catch (GatewayRequestHandler.AuthFailedException e) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, e.getMessage());
        }

        try (final ProduceGateway produceGateway =
                new ProduceGateway(
                        topicConnectionsRuntimeRegistryProvider
                                .getTopicConnectionsRuntimeRegistry(),
                        clusterRuntimeRegistry,
                        topicProducerCache)) {
            final List<Header> commonHeaders =
                    ProduceGateway.getProducerCommonHeaders(
                            context.gateway().getProduceOptions(), authContext);
            produceGateway.start(context.gateway().getTopic(), commonHeaders, authContext);
            final ProduceRequest produceRequest = parseProduceRequest(request, payload);
            produceGateway.produceMessage(produceRequest);
            return ProduceResponse.OK;
        }
    }

    private ProduceRequest parseProduceRequest(WebRequest request, String payload)
            throws ProduceGateway.ProduceException {
        final String contentType = request.getHeader("Content-Type");
        if (contentType == null || contentType.equals(MediaType.TEXT_PLAIN_VALUE)) {
            return new ProduceRequest(null, payload, null);
        } else if (contentType.equals(MediaType.APPLICATION_JSON_VALUE)) {
            return ProduceGateway.parseProduceRequest(payload);
        } else {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    String.format("Unsupported content type: %s", contentType));
        }
    }

    private Map<String, String> computeHeaders(WebRequest request) {
        final Map<String, String> headers = new HashMap<>();
        request.getHeaderNames()
                .forEachRemaining(name -> headers.put(name, request.getHeader(name)));
        return headers;
    }

    @PostMapping(value = GATEWAY_SERVICE_PATH)
    CompletableFuture<ResponseEntity> service(
            WebRequest request,
            HttpServletRequest servletRequest,
            @NotBlank @PathVariable("tenant") String tenant,
            @NotBlank @PathVariable("application") String application,
            @NotBlank @PathVariable("gateway") String gateway)
            throws Exception {
        return handleServiceCall(request, servletRequest, tenant, application, gateway);
    }

    @GetMapping(value = GATEWAY_SERVICE_PATH)
    CompletableFuture<ResponseEntity> serviceGet(
            WebRequest request,
            HttpServletRequest servletRequest,
            @NotBlank @PathVariable("tenant") String tenant,
            @NotBlank @PathVariable("application") String application,
            @NotBlank @PathVariable("gateway") String gateway)
            throws Exception {
        return handleServiceCall(request, servletRequest, tenant, application, gateway);
    }

    @PutMapping(value = GATEWAY_SERVICE_PATH)
    CompletableFuture<ResponseEntity> servicePut(
            WebRequest request,
            HttpServletRequest servletRequest,
            @NotBlank @PathVariable("tenant") String tenant,
            @NotBlank @PathVariable("application") String application,
            @NotBlank @PathVariable("gateway") String gateway)
            throws Exception {
        return handleServiceCall(request, servletRequest, tenant, application, gateway);
    }

    @DeleteMapping(value = GATEWAY_SERVICE_PATH)
    CompletableFuture<ResponseEntity> serviceDelete(
            WebRequest request,
            HttpServletRequest servletRequest,
            @NotBlank @PathVariable("tenant") String tenant,
            @NotBlank @PathVariable("application") String application,
            @NotBlank @PathVariable("gateway") String gateway)
            throws Exception {
        return handleServiceCall(request, servletRequest, tenant, application, gateway);
    }

    private CompletableFuture<ResponseEntity> handleServiceCall(
            WebRequest request,
            HttpServletRequest servletRequest,
            String tenant,
            String application,
            String gateway)
            throws IOException, ProduceGateway.ProduceException {
        final Map<String, String> queryString = computeQueryString(request);
        final Map<String, String> headers = computeHeaders(request);
        final GatewayRequestContext context =
                gatewayRequestHandler.validateRequest(
                        tenant,
                        application,
                        gateway,
                        Gateway.GatewayType.service,
                        queryString,
                        headers,
                        new GatewayRequestHandler.GatewayRequestValidator() {
                            @Override
                            public List<String> getAllRequiredParameters(Gateway gateway) {
                                return List.of();
                            }

                            @Override
                            public void validateOptions(Map<String, String> options) {}
                        });
        final AuthenticatedGatewayRequestContext authContext;
        try {
            authContext = gatewayRequestHandler.authenticate(context);
        } catch (GatewayRequestHandler.AuthFailedException e) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, e.getMessage());
        }
        if (context.gateway().getServiceOptions().getAgentId() != null) {
            final String uri =
                    applicationStore.getExecutorServiceURI(
                            context.tenant(),
                            context.applicationId(),
                            context.gateway().getServiceOptions().getAgentId());
            return forwardTo(uri, servletRequest.getMethod(), servletRequest);
        } else {
            if (!servletRequest.getMethod().equalsIgnoreCase("post")) {
                throw new ResponseStatusException(
                        HttpStatus.BAD_REQUEST, "Only POST method is supported");
            }
            final String payload =
                    new String(
                            servletRequest.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            final ProduceRequest produceRequest = parseProduceRequest(request, payload);
            return handleServiceWithTopics(produceRequest, authContext);
        }
    }

    private CompletableFuture<ResponseEntity> handleServiceWithTopics(
            ProduceRequest produceRequest, AuthenticatedGatewayRequestContext authContext) {

        final String langstreamServiceRequestId = UUID.randomUUID().toString();

        final CompletableFuture<ResponseEntity> completableFuture = new CompletableFuture<>();
        try (final ProduceGateway produceGateway =
                new ProduceGateway(
                        topicConnectionsRuntimeRegistryProvider
                                .getTopicConnectionsRuntimeRegistry(),
                        clusterRuntimeRegistry,
                        topicProducerCache); ) {

            final ConsumeGateway consumeGateway =
                    new ConsumeGateway(
                            topicConnectionsRuntimeRegistryProvider
                                    .getTopicConnectionsRuntimeRegistry(),
                            clusterRuntimeRegistry);
            completableFuture.thenRunAsync(consumeGateway::close, consumeThreadPool);

            final Gateway.ServiceOptions serviceOptions = authContext.gateway().getServiceOptions();
            try {
                final List<Function<Record, Boolean>> messageFilters =
                        ConsumeGateway.createMessageFilters(
                                serviceOptions.getHeaders(),
                                authContext.userParameters(),
                                authContext.principalValues());
                messageFilters.add(
                        record -> {
                            final Header header = record.getHeader(SERVICE_REQUEST_ID_HEADER);
                            if (header == null) {
                                return false;
                            }
                            return langstreamServiceRequestId.equals(header.valueAsString());
                        });
                consumeGateway.setup(serviceOptions.getOutputTopic(), messageFilters, authContext);
                final AtomicBoolean stop = new AtomicBoolean(false);
                consumeGateway.startReadingAsync(
                        consumeThreadPool,
                        stop::get,
                        record -> {
                            stop.set(true);
                            completableFuture.complete(ResponseEntity.ok(record));
                        });
            } catch (Exception ex) {
                log.error("Error while setting up consume gateway", ex);
                throw new RuntimeException(ex);
            }
            final List<Header> commonHeaders =
                    ProduceGateway.getProducerCommonHeaders(serviceOptions, authContext);
            produceGateway.start(serviceOptions.getInputTopic(), commonHeaders, authContext);

            Map<String, String> passedHeaders = produceRequest.headers();
            if (passedHeaders == null) {
                passedHeaders = new HashMap<>();
            }
            passedHeaders.put(SERVICE_REQUEST_ID_HEADER, langstreamServiceRequestId);
            produceGateway.produceMessage(
                    new ProduceRequest(
                            produceRequest.key(), produceRequest.value(), passedHeaders));
        } catch (Throwable t) {
            log.error("Error on service gateway", t);
            completableFuture.completeExceptionally(t);
        }
        return completableFuture;
    }

    private Map<String, String> computeQueryString(WebRequest request) {
        final Map<String, String> queryString =
                request.getParameterMap().keySet().stream()
                        .collect(Collectors.toMap(k -> k, k -> request.getParameter(k)));
        return queryString;
    }

    private CompletableFuture<ResponseEntity> forwardTo(
            String agentURI, String method, HttpServletRequest request) {
        try {
            String requestUrl = request.getRequestURI();
            final String[] parts = requestUrl.split("/", 8);
            final List<String> partsList =
                    Arrays.stream(parts).filter(s -> !s.isBlank()).collect(Collectors.toList());

            // /api/gateways/service/tenant/application/gateway/<part>
            if (partsList.size() > 6) {
                requestUrl = "/" + partsList.get(6);
            } else {
                requestUrl = "/";
            }
            final URI uri =
                    UriComponentsBuilder.fromUri(URI.create(agentURI))
                            .path(requestUrl)
                            .query(request.getQueryString())
                            .build(true)
                            .toUri();
            log.debug("Forwarding service request to {}, method {}", uri, method);

            final HttpRequest.Builder requestBuilder =
                    HttpRequest.newBuilder(uri)
                            .version(HttpClient.Version.HTTP_1_1)
                            .method(
                                    method,
                                    HttpRequest.BodyPublishers.ofInputStream(
                                            () -> {
                                                try {
                                                    return request.getInputStream();
                                                } catch (IOException e) {
                                                    throw new RuntimeException(e);
                                                }
                                            }));

            request.getHeaderNames()
                    .asIterator()
                    .forEachRemaining(
                            h -> {
                                switch (h) {
                                        // from jdk.internal.net.http.common.Utils
                                    case "connection":
                                    case "content-length":
                                    case "expect":
                                    case "host":
                                    case "upgrade":
                                        return;
                                    default:
                                        requestBuilder.header(h, request.getHeader(h));
                                        break;
                                }
                            });

            final HttpRequest httpRequest = requestBuilder.build();
            return httpClient
                    .sendAsync(httpRequest, HttpResponse.BodyHandlers.ofInputStream())
                    .thenApply(
                            remoteResponse -> {
                                final ResponseEntity.BodyBuilder responseBuilder =
                                        ResponseEntity.status(remoteResponse.statusCode());
                                remoteResponse
                                        .headers()
                                        .map()
                                        .forEach(
                                                (k, v) -> {
                                                    responseBuilder.header(k, v.get(0));
                                                });

                                return responseBuilder.body(
                                        new InputStreamResource(remoteResponse.body()));
                            });
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
    }
}
