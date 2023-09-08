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
import ai.langstream.cli.api.model.Gateways;
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
            Map<String, String> systemParams,
            Map<String, String> userParams,
            Map<String, String> options) {
        String paramsPart = "";
        String optionsPart = "";
        String systemParamsPart = "";
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

        if (systemParams != null) {
            systemParamsPart =
                    systemParams.entrySet().stream()
                            .map(e -> encodeParam(e, ""))
                            .collect(Collectors.joining("&"));
        }

        return String.join("&", List.of(systemParamsPart, paramsPart, optionsPart));
    }

    private static String encodeParam(Map.Entry<String, String> e, String prefix) {
        return encodeParam(e.getKey(), e.getValue(), prefix);
    }

    @SneakyThrows
    private static String encodeParam(String key, String value, String prefix) {
        return String.format(
                "%s=%s", prefix + key, URLEncoder.encode(value, StandardCharsets.UTF_8));
    }

    protected String validateGatewayAndGetUrl(
            String applicationId,
            String gatewayId,
            String type,
            Map<String, String> params,
            Map<String, String> options,
            String credentials,
            String adminCredentials,
            String adminCredentialsType,
            Map<String, String> adminCredentialsInputs) {
        validateGateway(
                applicationId,
                gatewayId,
                type,
                params,
                options,
                credentials,
                adminCredentials,
                adminCredentialsType,
                adminCredentialsInputs);

        Map<String, String> systemParams = new HashMap<>();
        if (credentials != null) {
            systemParams.put("credentials", credentials);
        }
        if (adminCredentials != null) {
            systemParams.put("admin-credentials", adminCredentials);
        }
        if (adminCredentialsType != null) {
            systemParams.put("admin-credentials-type", adminCredentialsType);
        }
        if (adminCredentialsInputs != null) {
            for (Map.Entry<String, String> adminInput : adminCredentialsInputs.entrySet()) {
                systemParams.put(
                        "admin-credentials-input-" + adminInput.getKey(), adminInput.getValue());
            }
        }

        return String.format(
                "%s/v1/%s/%s/%s/%s?%s",
                getApiGatewayUrl(),
                type,
                getTenant(),
                applicationId,
                gatewayId,
                computeQueryString(systemParams, params, options));
    }

    private String getTenant() {
        return getCurrentProfile().getTenant();
    }

    private String getApiGatewayUrl() {
        return getCurrentProfile().getApiGatewayUrl();
    }

    private Map<String, String> applicationDescriptions = new HashMap<>();

    @SneakyThrows
    protected void validateGateway(
            String application,
            String gatewayId,
            String type,
            Map<String, String> params,
            Map<String, String> options,
            String credentials,
            String adminCredentials,
            String adminCredentialsType,
            Map<String, String> adminCredentialsInputs) {

        final AdminClient client = getClient();

        final String applicationContent =
                applicationDescriptions.computeIfAbsent(
                        application, app -> client.applications().get(application, false));
        final List<Gateways.Gateway> gateways =
                Gateways.readFromApplicationDescription(applicationContent);

        if (gateways.isEmpty()) {
            throw new IllegalArgumentException("No gateway defined for application " + application);
        }

        Gateways.Gateway selectedGateway = null;
        for (Gateways.Gateway gateway : gateways) {
            if (gateway.getId().equals(gatewayId) && type.equals(gateway.getType())) {
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
        final List<String> requiredParameters = selectedGateway.getParameters();
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
        if (selectedGateway.getAuthentication() != null) {
            if (credentials == null && adminCredentials == null) {
                throw new IllegalArgumentException(
                        "gateway " + gatewayId + " of type " + type + " requires credentials");
            }
            if (adminCredentials != null) {
                final Object allowAdminRequests =
                        selectedGateway.getAuthentication().get("allow-admin-requests");
                if (allowAdminRequests != null && allowAdminRequests.toString().equals("false")) {
                    throw new IllegalArgumentException(
                            "gateway "
                                    + gatewayId
                                    + " of type "
                                    + type
                                    + " do not allow admin requests");
                }
            }
        }
    }
}
