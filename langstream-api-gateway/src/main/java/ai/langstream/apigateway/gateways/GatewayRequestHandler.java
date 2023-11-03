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

import ai.langstream.api.gateway.GatewayAuthenticationProvider;
import ai.langstream.api.gateway.GatewayAuthenticationProviderRegistry;
import ai.langstream.api.gateway.GatewayAuthenticationResult;
import ai.langstream.api.gateway.GatewayRequestContext;
import ai.langstream.api.model.Application;
import ai.langstream.api.model.ApplicationSpecs;
import ai.langstream.api.model.Gateway;
import ai.langstream.api.model.Gateways;
import ai.langstream.api.storage.ApplicationStore;
import ai.langstream.apigateway.config.GatewayTestAuthenticationProperties;
import ai.langstream.apigateway.websocket.AuthenticatedGatewayRequestContext;
import ai.langstream.apigateway.websocket.impl.AuthenticatedGatewayRequestContextImpl;
import ai.langstream.apigateway.websocket.impl.GatewayRequestContextImpl;
import ai.langstream.impl.common.ApplicationPlaceholderResolver;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.util.StringUtils;

@Slf4j
public class GatewayRequestHandler {

    public static class AuthFailedException extends Exception {
        public AuthFailedException(String message) {
            super(message);
        }
    }

    public interface GatewayRequestValidator {
        List<String> getAllRequiredParameters(Gateway gateway);

        void validateOptions(Map<String, String> options);
    }

    private final ApplicationStore applicationStore;
    private final GatewayAuthenticationProvider authTestProvider;

    public GatewayRequestHandler(
            ApplicationStore applicationStore,
            GatewayTestAuthenticationProperties testAuthenticationProperties) {
        this.applicationStore = applicationStore;
        if (testAuthenticationProperties.getType() != null) {
            authTestProvider =
                    GatewayAuthenticationProviderRegistry.loadProvider(
                            testAuthenticationProperties.getType(),
                            testAuthenticationProperties.getConfiguration());
            log.info(
                    "Loaded test authentication provider {}",
                    authTestProvider.getClass().getName());
        } else {
            authTestProvider = null;
            log.info("No test authentication provider configured");
        }
    }

    public GatewayRequestContext validateRequest(
            String tenant,
            String applicationId,
            String gatewayId,
            Gateway.GatewayType expectedGatewayType,
            Map<String, String> queryString,
            Map<String, String> httpHeaders,
            GatewayRequestValidator validator) {

        final Application application = getResolvedApplication(tenant, applicationId);
        final Gateway gateway = extractGateway(gatewayId, application, expectedGatewayType);

        final Map<String, String> options = new HashMap<>();
        final Map<String, String> userParameters = new HashMap<>();

        final String credentials = queryString.remove("credentials");
        final String testCredentials = queryString.remove("test-credentials");
        final boolean checkOptions;

        if (expectedGatewayType == Gateway.GatewayType.service
                && gateway.getServiceOptions().getAgentId() != null) {
            checkOptions = false;
        } else {
            checkOptions = true;
        }

        if (checkOptions) {
            for (Map.Entry<String, String> entry : queryString.entrySet()) {
                if (entry.getKey().startsWith("option:")) {
                    options.put(entry.getKey().substring("option:".length()), entry.getValue());
                } else if (entry.getKey().startsWith("param:")) {
                    userParameters.put(
                            entry.getKey().substring("param:".length()), entry.getValue());
                } else {
                    throw new IllegalArgumentException(
                            "invalid query parameter "
                                    + entry.getKey()
                                    + ". "
                                    + "To specify a gateway parameter, use the format param:<parameter_name>."
                                    + "To specify a option, use the format option:<option_name>.");
                }
            }
        }

        final List<String> requiredParameters = validator.getAllRequiredParameters(gateway);
        Set<String> allUserParameterKeys = new HashSet<>(userParameters.keySet());
        if (requiredParameters != null) {
            for (String requiredParameter : requiredParameters) {
                final String value = userParameters.get(requiredParameter);
                if (!StringUtils.hasText(value)) {
                    throw new IllegalArgumentException(
                            formatErrorMessage(
                                    tenant,
                                    applicationId,
                                    gateway,
                                    "missing required parameter "
                                            + requiredParameter
                                            + ". Required parameters: "
                                            + requiredParameters));
                }
                allUserParameterKeys.remove(requiredParameter);
            }
        }
        if (!allUserParameterKeys.isEmpty()) {
            throw new IllegalArgumentException(
                    formatErrorMessage(
                            tenant,
                            applicationId,
                            gateway,
                            "unknown parameters: " + allUserParameterKeys));
        }
        validator.validateOptions(options);

        if (credentials != null && testCredentials != null) {
            throw new IllegalArgumentException(
                    formatErrorMessage(
                            tenant,
                            applicationId,
                            gateway,
                            "credentials and test-credentials cannot be used together"));
        }
        return GatewayRequestContextImpl.builder()
                .tenant(tenant)
                .applicationId(applicationId)
                .application(application)
                .credentials(credentials)
                .testCredentials(testCredentials)
                .httpHeaders(httpHeaders)
                .options(options)
                .userParameters(userParameters)
                .gateway(gateway)
                .build();
    }

    private static String formatErrorMessage(
            String tenant, String applicationId, Gateway gateway, String error) {
        return "Error for gateway %s (tenant: %s, appId: %s): %s"
                .formatted(gateway.getId(), tenant, applicationId, error);
    }

    private Application getResolvedApplication(String tenant, String applicationId) {
        final ApplicationSpecs applicationSpecs = applicationStore.getSpecs(tenant, applicationId);
        if (applicationSpecs == null) {
            throw new IllegalArgumentException("application " + applicationId + " not found");
        }
        final Application application = applicationSpecs.getApplication();
        application.setSecrets(applicationStore.getSecrets(tenant, applicationId));
        return ApplicationPlaceholderResolver.resolvePlaceholders(application);
    }

    private Gateway extractGateway(
            String gatewayId, Application application, Gateway.GatewayType type) {
        final Gateways gatewaysObj = application.getGateways();
        if (gatewaysObj == null) {
            throw new IllegalArgumentException("no gateways defined for the application");
        }
        final List<Gateway> gateways = gatewaysObj.gateways();
        if (gateways == null) {
            throw new IllegalArgumentException("no gateways defined for the application");
        }

        Gateway selectedGateway = null;

        for (Gateway gateway : gateways) {
            if (gateway.getId().equals(gatewayId) && type == gateway.getType()) {
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
        return selectedGateway;
    }

    public AuthenticatedGatewayRequestContext authenticate(
            GatewayRequestContext gatewayRequestContext) throws AuthFailedException {

        final Gateway.Authentication authentication =
                gatewayRequestContext.gateway().getAuthentication();

        if (authentication == null) {
            return getAuthenticatedGatewayRequestContext(
                    gatewayRequestContext, Map.of(), new HashMap<>());
        }

        final GatewayAuthenticationResult result;
        if (gatewayRequestContext.isTestMode()) {
            if (!authentication.isAllowTestMode()) {
                throw new AuthFailedException(
                        "Gateway "
                                + gatewayRequestContext.gateway().getId()
                                + " of tenant "
                                + gatewayRequestContext.tenant()
                                + " does not allow test mode.");
            }
            if (authTestProvider == null) {
                throw new AuthFailedException("No test auth provider specified");
            }
            result = authTestProvider.authenticate(gatewayRequestContext);
        } else {
            final String provider = authentication.getProvider();
            final GatewayAuthenticationProvider authProvider =
                    GatewayAuthenticationProviderRegistry.loadProvider(
                            provider, authentication.getConfiguration());
            result = authProvider.authenticate(gatewayRequestContext);
        }
        if (result == null) {
            throw new AuthFailedException("Authentication provider returned null");
        }
        if (!result.authenticated()) {
            throw new AuthFailedException(result.reason());
        }
        final Map<String, String> principalValues =
                getPrincipalValues(result, gatewayRequestContext);
        return getAuthenticatedGatewayRequestContext(
                gatewayRequestContext, principalValues, new HashMap<>());
    }

    private Map<String, String> getPrincipalValues(
            GatewayAuthenticationResult result, GatewayRequestContext context) {
        if (!context.isTestMode()) {
            final Map<String, String> values = result.principalValues();
            if (values == null) {
                return Map.of();
            }
            return values;
        } else {
            final Map<String, String> values = new HashMap<>();
            final String principalSubject = DigestUtils.sha256Hex(context.credentials());
            final int principalNumericId = principalSubject.hashCode();
            final String principalEmail = "%s@locahost".formatted(principalSubject);

            // google
            values.putIfAbsent("subject", principalSubject);
            values.putIfAbsent("email", principalEmail);
            values.putIfAbsent("name", principalSubject);

            // github
            values.putIfAbsent("login", principalSubject);
            values.putIfAbsent("id", principalNumericId + "");
            return values;
        }
    }

    private AuthenticatedGatewayRequestContext getAuthenticatedGatewayRequestContext(
            GatewayRequestContext gatewayRequestContext,
            Map<String, String> principalValues,
            Map<String, Object> attributes) {

        return AuthenticatedGatewayRequestContextImpl.builder()
                .gatewayRequestContext(gatewayRequestContext)
                .attributes(attributes)
                .principalValues(principalValues)
                .build();
    }
}
