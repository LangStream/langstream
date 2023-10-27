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
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HttpAuthenticationProvider implements GatewayAuthenticationProvider {

    private static final ObjectMapper mapper = new ObjectMapper();
    private HttpAuthenticationProviderConfiguration httpConfiguration;
    private HttpClient httpClient;

    @Override
    public String type() {
        return "http";
    }

    @Override
    @SneakyThrows
    public void initialize(Map<String, Object> configuration) {
        httpConfiguration =
                mapper.convertValue(configuration, HttpAuthenticationProviderConfiguration.class);
        httpClient =
                HttpClient.newBuilder()
                        .connectTimeout(Duration.ofSeconds(30))
                        .followRedirects(HttpClient.Redirect.ALWAYS)
                        .build();
    }

    @Override
    public GatewayAuthenticationResult authenticate(GatewayRequestContext context) {

        final Map<String, String> placeholders = Map.of("tenant", context.tenant());
        final String uri = resolvePlaceholders(placeholders, httpConfiguration.getPathTemplate());
        final String url = httpConfiguration.getBaseUrl() + uri;

        log.info("Authenticating admin with url: {}", url);

        final HttpRequest.Builder builder = HttpRequest.newBuilder().uri(URI.create(url));

        httpConfiguration.getHeaders().forEach(builder::header);
        final String credentials = context.credentials();
        builder.header("Authorization", "Bearer " + (credentials == null ? "" : credentials));
        final HttpRequest request = builder.GET().build();

        final HttpResponse<Void> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (Throwable e) {
            return GatewayAuthenticationResult.authenticationFailed(e.getMessage());
        }
        if (httpConfiguration.getAcceptedStatuses().contains(response.statusCode())) {
            return GatewayAuthenticationResult.authenticationSuccessful(Map.of());
        }
        return GatewayAuthenticationResult.authenticationFailed(
                "Http authentication failed: " + response.statusCode());
    }

    private static String resolvePlaceholders(Map<String, String> placeholders, String url) {
        for (Map.Entry<String, String> entry : placeholders.entrySet()) {
            url = url.replace("{" + entry.getKey() + "}", entry.getValue());
        }
        return url;
    }
}
