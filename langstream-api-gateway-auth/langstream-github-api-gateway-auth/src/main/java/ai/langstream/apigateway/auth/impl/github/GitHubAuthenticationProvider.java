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
package ai.langstream.apigateway.auth.impl.github;

import ai.langstream.api.gateway.GatewayAuthenticationProvider;
import ai.langstream.api.gateway.GatewayAuthenticationResult;
import ai.langstream.api.gateway.GatewayRequestContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GitHubAuthenticationProvider implements GatewayAuthenticationProvider {

    private static final ObjectMapper mapper = new ObjectMapper();

    private String clientId;

    @Override
    public String type() {
        return "github";
    }

    @Override
    public void initialize(Map<String, Object> configuration) {
        final GitHubAuthenticationProviderConfiguration config =
                mapper.convertValue(configuration, GitHubAuthenticationProviderConfiguration.class);
        clientId = config.getClientId();
        log.info("Initialized GitHub authentication with configuration: {}", config);
    }

    @Override
    public GatewayAuthenticationResult authenticate(GatewayRequestContext context) {
        try {
            String token = context.credentials();

            /*
            curl --request GET \
            --url "https://api.github.com/user" \
            --header "Accept: application/vnd.github+json" \
            --header "Authorization: Bearer USER_ACCESS_TOKEN" \
            --header "X-GitHub-Api-Version: 2022-11-28"
            */

            if (token != null) {

                HttpClient client = HttpClient.newHttpClient();
                HttpRequest request =
                        HttpRequest.newBuilder()
                                .uri(URI.create("https://api.github.com/user"))
                                .header("Accept", "application/vnd.github+json")
                                .header("Authorization", "Bearer " + token)
                                .header("X-GitHub-Api-Version", "2022-11-28")
                                .build();

                HttpResponse<String> response =
                        client.send(request, HttpResponse.BodyHandlers.ofString());
                String body = response.body();
                String responseClientId =
                        response.headers().firstValue("X-OAuth-Client-Id").orElse(null);

                log.info("GitHub response: {}", body);
                log.info("X-OAuth-Client-Id: {}", responseClientId);
                log.info("Required: X-OAuth-Client-Id: {}", clientId);

                Map<String, String> result = mapper.readValue(body, Map.class);
                if (log.isDebugEnabled()) {
                    response.headers().map().forEach((k, v) -> log.debug("Header {}: {}", k, v));
                }

                if (clientId != null && !clientId.isEmpty()) {
                    if (!Objects.equals(responseClientId, clientId)) {
                        String message =
                                "Invalid client id,"
                                        + "the token has been issued by "
                                        + responseClientId
                                        + ", expecting "
                                        + clientId;
                        log.info(message);
                        return GatewayAuthenticationResult.authenticationFailed(message);
                    }
                }
                return GatewayAuthenticationResult.authenticationSuccessful(result);
            } else {
                return GatewayAuthenticationResult.authenticationFailed("Invalid token.");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
