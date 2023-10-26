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

import ai.langstream.cli.api.model.Gateways;
import ai.langstream.cli.commands.BaseCmd;
import ai.langstream.cli.commands.RootCmd;
import ai.langstream.cli.commands.RootGatewayCmd;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import picocli.CommandLine;

public abstract class BaseGatewayCmd extends BaseCmd {

    protected static final ObjectMapper messageMapper = new ObjectMapper();

    protected enum Protocols {
        ws,
        http;
    }

    @CommandLine.ParentCommand private RootGatewayCmd cmd;

    @Override
    protected RootCmd getRootCmd() {
        return cmd.getRootCmd();
    }

    private static String computeQueryString(
            Map<String, String> systemParams,
            Map<String, String> userParams,
            Map<String, String> options) {
        List<String> queryString = new ArrayList<>();
        if (userParams != null) {
            userParams.entrySet().stream()
                    .map(e -> encodeParam(e, "param:"))
                    .forEach(queryString::add);
        }

        if (options != null) {
            options.entrySet().stream()
                    .map(e -> encodeParam(e, "option:"))
                    .forEach(queryString::add);
        }

        if (systemParams != null) {
            systemParams.entrySet().stream().map(e -> encodeParam(e, "")).forEach(queryString::add);
        }

        return queryString.stream().collect(Collectors.joining("&"));
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
            String testCredentials,
            Protocols protocol) {
        validateGateway(
                applicationId, gatewayId, type, params, options, credentials, testCredentials);

        Map<String, String> systemParams = new HashMap<>();
        if (credentials != null) {
            systemParams.put("credentials", credentials);
        }
        if (testCredentials != null) {
            systemParams.put("test-credentials", testCredentials);
        }
        if (protocol == Protocols.http) {
            if (!type.equals("produce")) {
                throw new IllegalArgumentException("HTTP protocol is only supported for produce");
            }
            return String.format(
                    "%s/api/gateways/%s/%s/%s/%s?%s",
                    getApiGatewayUrlHttp(),
                    type,
                    getTenant(),
                    applicationId,
                    gatewayId,
                    computeQueryString(systemParams, params, options));
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

    private String getApiGatewayUrlHttp() {
        return getApiGatewayUrl().replace("wss://", "https://").replace("ws://", "http://");
    }

    @SneakyThrows
    protected void validateGateway(
            String application,
            String gatewayId,
            String type,
            Map<String, String> params,
            Map<String, String> options,
            String credentials,
            String testCredentials) {

        final String applicationContent = getAppDescriptionOrLoad(application);
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
            if (credentials == null && testCredentials == null) {
                throw new IllegalArgumentException(
                        "gateway " + gatewayId + " of type " + type + " requires credentials");
            }
            if (testCredentials != null) {
                final Object allowTestMode =
                        selectedGateway.getAuthentication().get("allow-test-mode");
                if (allowTestMode != null && allowTestMode.toString().equals("false")) {
                    throw new IllegalArgumentException(
                            "gateway "
                                    + gatewayId
                                    + " of type "
                                    + type
                                    + " do not allow test mode.");
                }
            }
        }
        if (credentials != null && testCredentials != null) {
            throw new IllegalArgumentException(
                    "credentials and test-credentials cannot be used together");
        }
    }

    protected Map<String, String> generatedParamsForChatGateway(
            String application, String gatewayId) {
        final String description = getAppDescriptionOrLoad(application);

        final List<Gateways.Gateway> gateways =
                Gateways.readFromApplicationDescription(description);

        if (gateways.isEmpty()) {
            return null;
        }

        Gateways.Gateway selectedGateway = null;
        for (Gateways.Gateway gateway : gateways) {
            if (gateway.getId().equals(gatewayId)
                    && Gateways.Gateway.TYPE_CHAT.equals(gateway.getType())) {
                selectedGateway = gateway;
                break;
            }
        }
        if (selectedGateway == null) {
            return null;
        }

        if (selectedGateway.getChatOptions() == null) {
            return null;
        }
        final List<Map<String, Object>> headers =
                (List<Map<String, Object>>) selectedGateway.getChatOptions().get("headers");
        if (headers == null) {
            return null;
        }

        Map<String, String> result = new HashMap<>();

        for (Map<String, Object> header : headers) {
            if (header.containsKey("valueFromParameters")) {
                final Object key = header.get("valueFromParameters");
                result.put(key.toString(), UUID.randomUUID().toString());
            }
        }
        return result;
    }
}
