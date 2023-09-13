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
package ai.langstream.auth.jwt;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LocalKubernetesJwksUriSigningKeyResolver {

    public static final String DEFAULT_TOKEN_PATH =
            "/var/run/secrets/kubernetes.io/serviceaccount/token";
    public static final String DEFAULT_K8S_BASE_URL =
            "https://kubernetes.default.svc.cluster.local";
    private final HttpClient httpClient;
    private final String token;
    private final String localK8sIssuer;
    private final Map<String, JwksUriSigningKeyResolver.JwksUri> cache = new ConcurrentHashMap<>();
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public LocalKubernetesJwksUriSigningKeyResolver(
            HttpClient httpClient, String tokenPath, String localIssuerBaseUrl) {
        this.httpClient = httpClient;
        token = loadToken(tokenPath);
        localK8sIssuer = loadLocalIssuer(localIssuerBaseUrl);
        log.info("Loaded local Kubernetes issuer: {}", localK8sIssuer);
    }

    public LocalKubernetesJwksUriSigningKeyResolver(HttpClient httpClient) {
        this(httpClient, DEFAULT_TOKEN_PATH, DEFAULT_K8S_BASE_URL);
    }

    @SneakyThrows
    private static String loadToken(String path) {
        if (path == null) {
            log.info(
                    "No token path specified. Kubernetes Service account authentication might not work.");
            return null;
        }
        final Path defaultPath = Path.of(path);
        if (Files.isRegularFile(defaultPath)) {
            log.info("Loading token from {}", defaultPath);
            return Files.readString(defaultPath);
        } else {
            log.info(
                    "No token found at {}. Kubernetes Service account authentication might not work.",
                    defaultPath);
        }
        return null;
    }

    @SneakyThrows
    private String loadLocalIssuer(String baseUrl) {
        if (baseUrl == null) {
            log.info(
                    "Base url not configured for local Kubernetes API. It's ok if not running in a kubernetes pod.");
            return null;
        }
        final String endpoint = composeWellKnownEndpoint(baseUrl);
        final Map<String, ?> response;
        try {
            response = getResponseFromWellKnownOpenIdConfiguration(endpoint);
        } catch (IOException connectException) {
            log.debug(
                    "Failed to connect to local Kubernetes API. It's ok if not running in a kubernetes pod.",
                    connectException);
            log.info(
                    "Failed to connect to local Kubernetes API. It's ok if not running in a kubernetes pod.");
            return null;
        }

        if (response != null) {
            Object issuer = response.get("issuer");
            if (issuer != null) {
                return issuer.toString();
            }
        }
        return null;
    }

    public JwksUriSigningKeyResolver.JwksUri getJwksUriFromIssuer(String issuer) {
        if (issuer == null) {
            log.debug("no issuer");
            return null;
        }
        if (!issuer.equals(localK8sIssuer)) {
            log.debug("issuer ({}) doesn't match local k8s issuer ({})", issuer, localK8sIssuer);
            return null;
        }

        final String kubeOpenIDUrl = composeWellKnownEndpoint(issuer);
        return cache.computeIfAbsent(kubeOpenIDUrl, this::getJwksUri);
    }

    private static String composeWellKnownEndpoint(String issuer) {
        final String kubeOpenIDUrl;
        if (issuer.endsWith("/")) {
            kubeOpenIDUrl = issuer + ".well-known/openid-configuration";
        } else {
            kubeOpenIDUrl = issuer + "/.well-known/openid-configuration";
        }
        return kubeOpenIDUrl;
    }

    private JwksUriSigningKeyResolver.JwksUri getJwksUri(String kubeOpenIDUrl) {
        try {
            final Map<String, ?> response =
                    getResponseFromWellKnownOpenIdConfiguration(kubeOpenIDUrl);

            if (response != null) {
                final Object jwksUri = response.get("jwks_uri");
                if (jwksUri != null) {
                    return new JwksUriSigningKeyResolver.JwksUri(jwksUri.toString(), false, token);
                }
            }
        } catch (IOException e) {
            log.warn("Failed to fetch keys from URL: {}", kubeOpenIDUrl, e);
        }
        return null;
    }

    private Map<String, ?> getResponseFromWellKnownOpenIdConfiguration(String kubeOpenIDUrl)
            throws IOException {

        final String value;
        final HttpResponse<String> httpResponse = sendGetRequest(kubeOpenIDUrl, false);
        if (httpResponse.statusCode() != 200) {
            // some kubernetes clusters require a token to access the well-known endpoint
            final HttpResponse<String> responseWithToken = sendGetRequest(kubeOpenIDUrl, true);
            if (responseWithToken.statusCode() != 200) {
                log.warn(
                        "Failed to fetch keys from URL: {}, got {} {}",
                        kubeOpenIDUrl,
                        httpResponse.statusCode(),
                        httpResponse.body());
                throw new IllegalStateException(
                        "Failed to fetch keys from URL: "
                                + kubeOpenIDUrl
                                + ", got "
                                + httpResponse.statusCode()
                                + " "
                                + httpResponse.body());
            }
            value = responseWithToken.body();
        } else {
            value = httpResponse.body();
        }
        return (Map<String, Object>) MAPPER.readValue(value, Map.class);
    }

    private HttpResponse<String> sendGetRequest(String kubeOpenIDUrl, boolean useToken)
            throws IOException {
        final HttpRequest.Builder builder =
                HttpRequest.newBuilder()
                        .uri(URI.create(kubeOpenIDUrl))
                        .version(HttpClient.Version.HTTP_1_1)
                        .GET();
        if (useToken) {
            builder.header("Authorization", "Bearer " + token);
        }
        final HttpResponse<String> httpResponse;
        try {
            httpResponse = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        return httpResponse;
    }
}
