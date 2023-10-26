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
package ai.langstream.apigateway.auth.impl.jwt.admin;

import ai.langstream.api.gateway.GatewayAuthenticationProvider;
import ai.langstream.api.gateway.GatewayAuthenticationResult;
import ai.langstream.api.gateway.GatewayRequestContext;
import ai.langstream.auth.jwt.AuthenticationProviderToken;
import ai.langstream.auth.jwt.JwtProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;

public class JwtAuthenticationProvider implements GatewayAuthenticationProvider {

    private static final ObjectMapper mapper = new ObjectMapper();
    private AuthenticationProviderToken authenticationProviderToken;
    private List<String> adminRoles;

    @Override
    public String type() {
        return "jwt";
    }

    @Override
    @SneakyThrows
    public void initialize(Map<String, Object> configuration) {
        final JwtAuthenticationProviderConfiguration tokenProperties =
                mapper.convertValue(configuration, JwtAuthenticationProviderConfiguration.class);

        if (tokenProperties.adminRoles() != null) {
            this.adminRoles = tokenProperties.adminRoles();
        } else {
            this.adminRoles = List.of();
        }

        final JwtProperties jwtProperties =
                new JwtProperties(
                        tokenProperties.secretKey(),
                        tokenProperties.publicKey(),
                        tokenProperties.authClaim(),
                        tokenProperties.publicAlg(),
                        tokenProperties.audienceClaim(),
                        tokenProperties.audience(),
                        tokenProperties.jwksHostsAllowlist(),
                        false,
                        null);
        this.authenticationProviderToken = new AuthenticationProviderToken(jwtProperties);
    }

    @Override
    public GatewayAuthenticationResult authenticate(GatewayRequestContext context) {
        String role;
        try {
            final String credentials = context.credentials();
            role = authenticationProviderToken.authenticate(credentials == null ? "" : credentials);
        } catch (AuthenticationProviderToken.AuthenticationException ex) {
            return GatewayAuthenticationResult.authenticationFailed(ex.getMessage());
        }
        if (!adminRoles.contains(role)) {
            return GatewayAuthenticationResult.authenticationFailed("Not an admin.");
        }
        return GatewayAuthenticationResult.authenticationSuccessful(Map.of());
    }
}
