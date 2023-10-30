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
import ai.langstream.apigateway.api.ConsumePushMessage;
import ai.langstream.apigateway.gateways.ConsumeGateway;
import ai.langstream.apigateway.gateways.GatewayRequestHandler;
import ai.langstream.apigateway.gateways.ProduceGateway;
import ai.langstream.apigateway.runner.TopicConnectionsRuntimeProviderBean;
import ai.langstream.apigateway.websocket.AuthenticatedGatewayRequestContext;
import ai.langstream.apigateway.api.ProduceRequest;
import ai.langstream.apigateway.api.ProduceResponse;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.validation.constraints.NotBlank;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/gateways")
@Slf4j
@AllArgsConstructor
public class GatewayResource {

    private final TopicConnectionsRuntimeProviderBean topicConnectionsRuntimeRegistryProvider;
    private final GatewayRequestHandler gatewayRequestHandler;
    private final ExecutorService consumeThreadPool = Executors.newCachedThreadPool();

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
                                     .getTopicConnectionsRuntimeRegistry());) {
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
            value = "/service/{tenant}/{application}/{gateway}",
            consumes = MediaType.APPLICATION_JSON_VALUE)
    void service(
            WebRequest request,
            HttpServletResponse response,
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
                        Gateway.GatewayType.service,
                        queryString,
                        headers,
                        new ProduceGateway.ProduceGatewayRequestValidator());
        final AuthenticatedGatewayRequestContext authContext;
        try {
            authContext = gatewayRequestHandler.authenticate(context);
        } catch (GatewayRequestHandler.AuthFailedException e) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, e.getMessage());
        }


        try (final ConsumeGateway consumeGateway = new ConsumeGateway(
                topicConnectionsRuntimeRegistryProvider
                        .getTopicConnectionsRuntimeRegistry());
             final ProduceGateway produceGateway =
                     new ProduceGateway(
                             topicConnectionsRuntimeRegistryProvider
                                     .getTopicConnectionsRuntimeRegistry());) {

            final Gateway.ServiceOptions serviceOptions = authContext.gateway().getServiceOptions();
            try {
                final List<Function<Record, Boolean>> messageFilters =
                        ConsumeGateway.createMessageFilters(
                                serviceOptions.getHeaders(), authContext.userParameters(),
                                authContext.principalValues());
                consumeGateway.setup(serviceOptions.getInputTopic(), messageFilters, authContext);
                final AtomicBoolean stop = new AtomicBoolean(false);
                consumeGateway.startReadingAsync(consumeThreadPool, () -> stop.get(), record -> {
                    stop.set(true);
                    try {
                        response.getWriter().print(record);
                        response.getWriter().flush();
                        response.getWriter().close();
                    } catch (IOException ioException) {
                        throw new RuntimeException(ioException);
                    }
                });
            } catch (Exception ex) {
                log.error("Error while setting up consume gateway", ex);
                throw new RuntimeException(ex);
            }
            final List<Header> commonHeaders =
                    ProduceGateway.getProducerCommonHeaders(serviceOptions, authContext);
            produceGateway.start(serviceOptions.getOutputTopic(), commonHeaders, authContext);
            produceGateway.produceMessage(produceRequest);
        }

    }

    private Map<String, String> computeQueryString(WebRequest request) {
        final Map<String, String> queryString =
                request.getParameterMap().keySet().stream()
                        .collect(Collectors.toMap(k -> k, k -> request.getParameter(k)));
        return queryString;
    }
}
