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
package ai.langstream.webservice.security.infrastructure.primary;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwsHeader;
import io.jsonwebtoken.Jwt;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SigningKeyResolver;
import io.jsonwebtoken.io.Decoders;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Key;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.RSAPublicKeySpec;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
public class LocalKubernetesJwksUriSigningKeyResolver {

    private final HttpClient httpClient;
    private final String token;
    private final String localK8sIssuer;
    private final Map<String, JwksUriSigningKeyResolver.JwksUri> cache = new ConcurrentHashMap<>();
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public LocalKubernetesJwksUriSigningKeyResolver(HttpClient httpClient) {
        this.httpClient = httpClient;
        token = loadToken();
        localK8sIssuer = loadLocalIssuer();
        log.info("Loaded local Kubernetes issuer: {}", localK8sIssuer);
    }

    @SneakyThrows
    private String loadToken() {
        final Path defaultPath = Path.of("/var/run/secrets/kubernetes.io/serviceaccount/token");
        if (Files.isRegularFile(defaultPath)) {
            log.info("Loading token from {}", defaultPath);
            return Files.readString(defaultPath);
        } else {
            log.info("No token found at {}. Service account authentication might not work.", defaultPath);
        }
        return null;
    }

    @SneakyThrows
    private String loadLocalIssuer() {
        final String endpoint = composeWellKnownEndpoint("https://kubernetes.default.svc.cluster.local");
        final Map<String, ?> response = getResponseFromWellKnownOpenIdConfiguration(endpoint);

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

    @NotNull
    private String composeWellKnownEndpoint(String issuer) {
        final String kubeOpenIDUrl;
        if (issuer.endsWith("/")) {
            kubeOpenIDUrl = issuer + ".well-known/openid-configuration";
        } else {
            kubeOpenIDUrl = issuer + "/.well-known/openid-configuration";
        }
        return kubeOpenIDUrl;
    }

    @Nullable
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

    private Map<String, ?> getResponseFromWellKnownOpenIdConfiguration(String kubeOpenIDUrl) throws IOException {

        final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(kubeOpenIDUrl))
                .version(HttpClient.Version.HTTP_1_1)
                .header("Authorization", "Bearer " + token)
                .GET()
                .build();

        final HttpResponse<String> httpResponse;
        try {
            httpResponse = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        if (httpResponse.statusCode() != 200) {
            throw new IllegalStateException("Failed to fetch keys from URL: " + kubeOpenIDUrl + ", got status code: " + httpResponse.statusCode());
        }
        final String value = httpResponse.body();
        return (Map<String, Object>) MAPPER.readValue(value, Map.class);
    }
}
