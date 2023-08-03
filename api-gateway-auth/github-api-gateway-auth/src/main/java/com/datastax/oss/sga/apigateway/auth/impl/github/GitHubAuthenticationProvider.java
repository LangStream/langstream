/**
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
package com.datastax.oss.sga.apigateway.auth.impl.github;

import com.datastax.oss.sga.api.gateway.GatewayAuthenticationProvider;
import com.datastax.oss.sga.api.gateway.GatewayAuthenticationResult;
import com.datastax.oss.sga.api.gateway.GatewayRequestContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

@Slf4j
public class GitHubAuthenticationProvider implements GatewayAuthenticationProvider {

    private static final ObjectMapper mapper = new ObjectMapper();


    @Override
    public String type() {
        return "github";
    }

    @Override
    public void initialize(Map<String, Object> configuration) {
        final GitHubAuthenticationProviderConfiguration config =
                mapper.convertValue(configuration, GitHubAuthenticationProviderConfiguration.class);
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
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create("https://api.github.com/user"))
                        .header("Accept", "application/vnd.github+json")
                        .header("Authorization", "Bearer " + token)
                        .header("X-GitHub-Api-Version", "2022-11-28")
                        .build();

                String body = client.send(request, HttpResponse.BodyHandlers.ofString()).body();
                log.info("GitHub response: {}", body);
                Map<String, String> result = new ObjectMapper().readValue(body, Map.class);

                return GatewayAuthenticationResult.authenticationSuccessful(result);
            } else {
                return GatewayAuthenticationResult.authenticationFailed("Invalid token.");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
