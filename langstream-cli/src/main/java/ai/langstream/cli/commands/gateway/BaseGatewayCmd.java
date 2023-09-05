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
package ai.langstream.cli.commands.gateway;

import ai.langstream.admin.client.AdminClient;
import ai.langstream.api.model.Gateway;
import ai.langstream.api.model.Gateways;
import ai.langstream.api.webservice.application.ApplicationDescription;
import ai.langstream.cli.commands.BaseCmd;
import ai.langstream.cli.commands.RootCmd;
import ai.langstream.cli.commands.RootGatewayCmd;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import picocli.CommandLine;

public abstract class BaseGatewayCmd extends BaseCmd {

    protected static final ObjectMapper messageMapper = new ObjectMapper();

    @CommandLine.ParentCommand private RootGatewayCmd cmd;

    @Override
    protected RootCmd getRootCmd() {
        return cmd.getRootCmd();
    }

    private static String computeQueryString(
            String credentials, Map<String, String> userParams, Map<String, String> options) {
        String paramsPart = "";
        String optionsPart = "";
        if (userParams != null) {
            paramsPart =
                    userParams.entrySet().stream()
                            .map(e -> encodeParam(e, "param:"))
                            .collect(Collectors.joining("&"));
        }

        if (options != null) {
            optionsPart =
                    options.entrySet().stream()
                            .map(e -> encodeParam(e, "option:"))
                            .collect(Collectors.joining("&"));
        }

        String credentialsPart = "";
        if (credentials != null) {
            credentialsPart = encodeParam("credentials", credentials, "");
        }

        return String.join("&", List.of(credentialsPart, paramsPart, optionsPart));
    }

    private static String encodeParam(Map.Entry<String, String> e, String prefix) {
        return encodeParam(e.getKey(), e.getValue(), prefix);
    }

    @SneakyThrows
    private static String encodeParam(String key, String value, String prefix) {
        return "%s=%s".formatted(prefix + key, URLEncoder.encode(value, StandardCharsets.UTF_8));
    }

    protected String validateGatewayAndGetUrl(
            String applicationId,
            String gatewayId,
            Gateway.GatewayType type,
            Map<String, String> params,
            Map<String, String> options,
            String credentials) {
        validateGateway(applicationId, gatewayId, type, params, options, credentials);
        return "%s/v1/%s/%s/%s/%s?%s"
                .formatted(
                        getConfig().getApiGatewayUrl(),
                        type.toString(),
                        getConfig().getTenant(),
                        applicationId,
                        gatewayId,
                        computeQueryString(credentials, params, options));
    }

    private Map<String, String> applicationDescriptions = new HashMap<>();

    @SneakyThrows
    protected void validateGateway(
            String application,
            String gatewayId,
            Gateway.GatewayType type,
            Map<String, String> params,
            Map<String, String> options,
            String credentials) {
        log("Validating gateway %s of type %s".formatted(gatewayId, type));
        final AdminClient client = getClient();

        final String applicationContent =
                applicationDescriptions.computeIfAbsent(
                        application, app -> client.applications().describe(application));

        final ApplicationDescription applicationDescription =
                messageMapper.readValue(applicationContent, ApplicationDescription.class);
        final Gateways gatewaysWrapper = applicationDescription.getApplication().getGateways();
        if (gatewaysWrapper == null
                || gatewaysWrapper.gateways() == null
                || gatewaysWrapper.gateways().isEmpty()) {
            throw new IllegalArgumentException("No gateway defined for application " + application);
        }

        Gateway selectedGateway = null;
        for (Gateway gateway : gatewaysWrapper.gateways()) {
            if (gateway.id().equals(gatewayId) && type == gateway.type()) {
                selectedGateway = gateway;
                break;
            }
        }
        if (selectedGateway == null) {
            throw new IllegalArgumentException(
                    "gateway "
                            + gatewayId
                            + " of type "
                            + type
                            + " is not defined in the application");
        }
        final List<String> requiredParameters = selectedGateway.parameters();
        if (requiredParameters != null) {
            for (String requiredParameter : requiredParameters) {
                if (params == null || !params.containsKey(requiredParameter)) {
                    throw new IllegalArgumentException(
                            "gateway "
                                    + gatewayId
                                    + " of type "
                                    + type
                                    + " requires parameter "
                                    + requiredParameter);
                }
            }
        }
        if (selectedGateway.authentication() != null) {
            if (credentials == null) {
                throw new IllegalArgumentException(
                        "gateway " + gatewayId + " of type " + type + " requires credentials");
            }
        }
    }
}
