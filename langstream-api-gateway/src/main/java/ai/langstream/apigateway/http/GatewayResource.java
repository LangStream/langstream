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
import ai.langstream.apigateway.gateways.GatewayRequestHandler;
import ai.langstream.apigateway.gateways.ProduceGateway;
import ai.langstream.apigateway.runner.TopicConnectionsRuntimeProviderBean;
import ai.langstream.apigateway.websocket.AuthenticatedGatewayRequestContext;
import ai.langstream.apigateway.websocket.api.ProduceRequest;
import ai.langstream.apigateway.websocket.api.ProduceResponse;
import jakarta.validation.constraints.NotBlank;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

        final Map<String, String> queryString =
                request.getParameterMap().keySet().stream()
                        .collect(Collectors.toMap(k -> k, k -> request.getParameter(k)));
        final Map<String, String> headers = new HashMap<>();
        request.getHeaderNames()
                .forEachRemaining(name -> headers.put(name, request.getHeader(name)));
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

        final ProduceGateway produceGateway =
                new ProduceGateway(
                        topicConnectionsRuntimeRegistryProvider
                                .getTopicConnectionsRuntimeRegistry());
        try {
            final List<Header> commonHeaders =
                    ProduceGateway.getProducerCommonHeaders(
                            context.gateway().getProduceOptions(), authContext);
            produceGateway.start(context.gateway().getTopic(), commonHeaders, authContext);
            produceGateway.produceMessage(produceRequest);
            return ProduceResponse.OK;
        } finally {
            produceGateway.close();
        }
    }
}
