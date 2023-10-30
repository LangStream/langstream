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
import ai.langstream.api.storage.ApplicationStore;
import ai.langstream.apigateway.api.ProduceRequest;
import ai.langstream.apigateway.api.ProduceResponse;
import ai.langstream.apigateway.gateways.ConsumeGateway;
import ai.langstream.apigateway.gateways.GatewayRequestHandler;
import ai.langstream.apigateway.gateways.ProduceGateway;
import ai.langstream.apigateway.gateways.TopicProducerCache;
import ai.langstream.apigateway.runner.TopicConnectionsRuntimeProviderBean;
import ai.langstream.apigateway.websocket.AuthenticatedGatewayRequestContext;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.validation.constraints.NotBlank;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.ws.rs.core.Response;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.util.UriComponentsBuilder;

@RestController
@RequestMapping("/api/gateways")
@Slf4j
@AllArgsConstructor
public class GatewayResource {

    protected static final String GATEWAY_SERVICE_PATH = "/service/{tenant}/{application}/{gateway}/**";
    private final TopicConnectionsRuntimeProviderBean topicConnectionsRuntimeRegistryProvider;
    private final TopicProducerCache topicProducerCache;
    private final ApplicationStore applicationStore;
    private final GatewayRequestHandler gatewayRequestHandler;

    @PostMapping(
            value = "/produce/{tenant}/{application}/{gateway}",
            consumes = MediaType.APPLICATION_JSON_VALUE)
    ProduceResponse produce(
            WebRequest request,
            @NotBlank @PathVariable("tenant") String tenant,
            @NotBlank @PathVariable("application") String application,
            @NotBlank @PathVariable("gateway") String gateway,
            @RequestBody ProduceRequest produceRequest)
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
                             topicProducerCache)) {
            final List<Header> commonHeaders =
                    ProduceGateway.getProducerCommonHeaders(
                            context.gateway().getProduceOptions(), authContext);
            produceGateway.start(context.gateway().getTopic(), commonHeaders, authContext);
            produceGateway.produceMessage(produceRequest);
            return ProduceResponse.OK;
        }
    }

    private Map<String, String> computeHeaders(WebRequest request) {
        final Map<String, String> headers = new HashMap<>();
        request.getHeaderNames()
                .forEachRemaining(name -> headers.put(name, request.getHeader(name)));
        return headers;
    }

    @PostMapping(
            value = GATEWAY_SERVICE_PATH)
    CompletableFuture<ResponseEntity> service(
            WebRequest request,
            HttpServletRequest servletRequest,
            @NotBlank @PathVariable("tenant") String tenant,
            @NotBlank @PathVariable("application") String application,
            @NotBlank @PathVariable("gateway") String gateway,
            @RequestBody String inputStream)
            throws Exception {
        return handleServiceCall(request, servletRequest, tenant, application, gateway, inputStream);
    }

    @GetMapping(
            value = GATEWAY_SERVICE_PATH)
    CompletableFuture<ResponseEntity> serviceGet(
            WebRequest request,
            HttpServletRequest servletRequest,
            @NotBlank @PathVariable("tenant") String tenant,
            @NotBlank @PathVariable("application") String application,
            @NotBlank @PathVariable("gateway") String gateway,
            @RequestBody String inputStream)
            throws Exception {
        return handleServiceCall(request, servletRequest, tenant, application, gateway, inputStream);
    }

    @PutMapping(
            value = GATEWAY_SERVICE_PATH)
    CompletableFuture<ResponseEntity> servicePut(
            WebRequest request,
            HttpServletRequest servletRequest,
            @NotBlank @PathVariable("tenant") String tenant,
            @NotBlank @PathVariable("application") String application,
            @NotBlank @PathVariable("gateway") String gateway,
            @RequestBody String inputStream)
            throws Exception {
        return handleServiceCall(request, servletRequest, tenant, application, gateway, inputStream);
    }

    @DeleteMapping(
            value = GATEWAY_SERVICE_PATH)
    CompletableFuture<ResponseEntity> serviceDelete(
            WebRequest request,
            HttpServletRequest servletRequest,
            @NotBlank @PathVariable("tenant") String tenant,
            @NotBlank @PathVariable("application") String application,
            @NotBlank @PathVariable("gateway") String gateway,
            @RequestBody String inputStream)
            throws Exception {
        return handleServiceCall(request, servletRequest, tenant, application, gateway, inputStream);
    }


    private CompletableFuture<ResponseEntity> handleServiceCall(WebRequest request,
                                                                                 HttpServletRequest servletRequest,
                                                                                 String tenant, String application,
                                                                                 String gateway, String inputStream)
            throws JsonProcessingException {
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
                            public void validateOptions(Map<String, String> options) {
                            }
                        });
        final AuthenticatedGatewayRequestContext authContext;
        try {
            authContext = gatewayRequestHandler.authenticate(context);
        } catch (GatewayRequestHandler.AuthFailedException e) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, e.getMessage());
        }
        if (context.gateway().getServiceOptions().getAgentId() != null) {
            final String uri =
                    applicationStore.getExecutorServiceURI(context.tenant(), context.applicationId(),
                            context.gateway().getServiceOptions().getAgentId());
            final ResponseEntity responseEntity = forwardTo(uri, inputStream, HttpMethod.valueOf(servletRequest.getMethod().toUpperCase(
                    Locale.ROOT)), servletRequest);
            return CompletableFuture.completedFuture(responseEntity);
        } else {
            if (!servletRequest.getMethod().equalsIgnoreCase("post")) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Only POST method is supported");
            }
            final ProduceRequest produceRequest = new ObjectMapper()
                    .readValue(inputStream, ProduceRequest.class);
            return handleServiceWithTopics(produceRequest, authContext);
        }
    }

    private CompletableFuture<ResponseEntity> handleServiceWithTopics(ProduceRequest produceRequest,
                                         AuthenticatedGatewayRequestContext authContext) {
        final CompletableFuture<ResponseEntity> completableFuture = new CompletableFuture<>();
        try (final ConsumeGateway consumeGateway =
                     new ConsumeGateway(
                             topicConnectionsRuntimeRegistryProvider
                                     .getTopicConnectionsRuntimeRegistry());
             final ProduceGateway produceGateway =
                     new ProduceGateway(
                             topicConnectionsRuntimeRegistryProvider
                                     .getTopicConnectionsRuntimeRegistry(),
                             topicProducerCache);) {

            final Gateway.ServiceOptions serviceOptions = authContext.gateway().getServiceOptions();
            try {
                final List<Function<Record, Boolean>> messageFilters =
                        ConsumeGateway.createMessageFilters(
                                serviceOptions.getHeaders(),
                                authContext.userParameters(),
                                authContext.principalValues());
                consumeGateway.setup(serviceOptions.getInputTopic(), messageFilters, authContext);
                final AtomicBoolean stop = new AtomicBoolean(false);
                consumeGateway.startReading(() -> stop.get(),
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
            produceGateway.start(serviceOptions.getOutputTopic(), commonHeaders, authContext);
            produceGateway.produceMessage(produceRequest);
        } catch (Throwable t) {
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


    public ResponseEntity forwardTo(String agentURI, String body, HttpMethod method, HttpServletRequest request) {
        String requestUrl = request.getRequestURI();
        final String[] parts = requestUrl.split("/", 8);
        final List<String> partsList = Arrays.stream(parts).filter(s -> !s.isBlank()).collect(Collectors.toList());
        log.info("partsList: {} -> {}", requestUrl, partsList);

        // /api/gateways/service/tenant/application/gateway/<part>
        if (partsList.size() > 6) {
            requestUrl = "/" + partsList.get(6);
        } else {
            requestUrl = "/";
        }
        final URI uri = UriComponentsBuilder.fromUri(URI.create(agentURI))
                .path(requestUrl)
                .query(request.getQueryString())
                .build(true).toUri();
        log.info("Forwarding service request to {}", uri);

        HttpHeaders headers = new HttpHeaders();
        Enumeration<String> headerNames = request.getHeaderNames();
        while (headerNames.hasMoreElements()) {
            String headerName = headerNames.nextElement();
            headers.set(headerName, request.getHeader(headerName));
        }

        HttpEntity<String> httpEntity = new HttpEntity<>(body, headers);
        RestTemplate restTemplate = new RestTemplate();
        try {
            return restTemplate.exchange(uri, method, httpEntity, String.class);
        } catch (HttpStatusCodeException e) {
            return ResponseEntity.status(e.getStatusCode().value())
                    .headers(e.getResponseHeaders())
                    .body(e.getResponseBodyAsString());
        }
    }
}
