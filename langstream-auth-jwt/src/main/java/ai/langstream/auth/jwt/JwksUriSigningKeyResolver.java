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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwsHeader;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.SigningKeyResolver;
import io.jsonwebtoken.io.Decoders;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
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
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JwksUriSigningKeyResolver implements SigningKeyResolver {

    public record JwksUri(String uri, boolean checkHost, String token) {}

    public record JwksUriCacheKey(JwksUri uri, String keyId) {}

    private static final Logger log = LoggerFactory.getLogger(JwksUriSigningKeyResolver.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String algorithm;
    private final Pattern hostsAllowlist;
    private final Key fallbackKey;
    private final HttpClient httpClient;
    private final LocalKubernetesJwksUriSigningKeyResolver localKubernetesJwksUriSigningKeyResolver;
    private Map<JwksUriCacheKey, Key> keyMap = new ConcurrentHashMap<>();

    public JwksUriSigningKeyResolver(String algorithm, String hostsAllowlist, Key fallbackKey) {
        this(algorithm, hostsAllowlist, fallbackKey, null);
    }

    public JwksUriSigningKeyResolver(
            String algorithm,
            String hostsAllowlist,
            Key fallbackKey,
            LocalKubernetesJwksUriSigningKeyResolver localKubernetesJwksUriSigningKeyResolver) {
        this.algorithm = algorithm;
        if (StringUtils.isBlank(hostsAllowlist)) {
            this.hostsAllowlist = null;
        } else {
            this.hostsAllowlist = Pattern.compile(hostsAllowlist);
        }
        this.fallbackKey = fallbackKey;
        this.httpClient =
                HttpClient.newBuilder()
                        .connectTimeout(Duration.ofSeconds(30))
                        .followRedirects(HttpClient.Redirect.ALWAYS)
                        .build();
        if (localKubernetesJwksUriSigningKeyResolver == null) {
            this.localKubernetesJwksUriSigningKeyResolver =
                    new LocalKubernetesJwksUriSigningKeyResolver(httpClient);
        } else {
            this.localKubernetesJwksUriSigningKeyResolver =
                    localKubernetesJwksUriSigningKeyResolver;
        }
    }

    @Override
    public Key resolveSigningKey(JwsHeader header, Claims claims) {
        JwksUri jwksUri = null;
        String tokenUri = (String) claims.get("jwks_uri");
        if (tokenUri != null) {
            jwksUri = new JwksUri(tokenUri, true, null);
        } else {
            final String issuer = claims.getIssuer();
            if (issuer != null) {
                log.debug("No jwks_uri claim in JWT, checking issuer");
                jwksUri = localKubernetesJwksUriSigningKeyResolver.getJwksUriFromIssuer(issuer);
                if (jwksUri == null) {
                    log.debug("Not able to get jwks_uri from issuer {}", issuer);
                } else {
                    log.debug("Got jwks_uri from issuer {}: {}", issuer, jwksUri.uri());
                }
            }
        }
        if (jwksUri == null) {
            log.debug("No jwks_uri claim in JWT, using fallback key");
            return fallbackKey;
        }
        return getKey(new JwksUriCacheKey(jwksUri, header.getKeyId()));
    }

    @Override
    public Key resolveSigningKey(JwsHeader header, String plaintext) {
        throw new UnsupportedOperationException();
    }

    private Key getKey(JwksUriCacheKey uri) {
        return keyMap.computeIfAbsent(uri, this::fetchKey);
    }

    private Key fetchKey(JwksUriCacheKey jwksKey) {
        final JwksUri jwksUri = jwksKey.uri();
        final String uri = jwksUri.uri();
        try {
            final URL src = new URL(uri);
            if (jwksUri.checkHost()) {
                if (hostsAllowlist == null || !hostsAllowlist.matcher(src.getHost()).matches()) {
                    throw new JwtException("Untrusted hostname: '" + src.getHost() + "'");
                }
            }
            final JwkKeys keys = getKeys(jwksUri);
            for (JwkKey key : keys.keys()) {
                if (!algorithm.equals(key.alg())) {
                    continue;
                }
                // no kid requested, use the first one
                if (jwksKey.keyId() != null && !jwksKey.keyId().equals(key.kid())) {
                    continue;
                }
                BigInteger modulus = base64ToBigInteger(key.n());
                BigInteger exponent = base64ToBigInteger(key.e());
                RSAPublicKeySpec rsaPublicKeySpec = new RSAPublicKeySpec(modulus, exponent);
                try {
                    KeyFactory keyFactory = KeyFactory.getInstance("RSA");
                    return keyFactory.generatePublic(rsaPublicKeySpec);
                } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
                    throw new JwtException(
                            "Failed to parse public key '" + key.kid() + "' from " + jwksUri.uri());
                }
            }
            throw new JwtException(
                    "No valid keys found from URL: " + uri + ", keyId: " + jwksKey.keyId());
        } catch (IOException e) {
            log.error("Failed to fetch keys from URL: {}", uri, e);
            throw new JwtException("Failed to fetch keys from URL: " + uri, e);
        }
    }

    private JwkKeys getKeys(JwksUri jwksUri) throws IOException {
        final HttpResponse<String> httpResponse = sendGetKeysRequest(jwksUri, false);
        final String body;
        if (httpResponse.statusCode() != 200) {
            if (jwksUri.token() != null) {
                final HttpResponse<String> responseWithToken = sendGetKeysRequest(jwksUri, true);
                if (responseWithToken.statusCode() != 200) {
                    log.warn(
                            "Failed to fetch keys from URL: {}, got {} {}",
                            jwksUri.uri(),
                            responseWithToken.statusCode(),
                            responseWithToken.body());
                    throw new IllegalStateException(
                            "Failed to fetch keys from URL: "
                                    + jwksUri.uri()
                                    + ", got "
                                    + responseWithToken.statusCode()
                                    + " "
                                    + responseWithToken.body());
                } else {
                    body = responseWithToken.body();
                }
            } else {
                log.warn(
                        "Failed to fetch keys from URL: {}, got {} {}",
                        jwksUri.uri(),
                        httpResponse.statusCode(),
                        httpResponse.body());
                throw new IllegalStateException(
                        "Failed to fetch keys from URL: "
                                + jwksUri.uri()
                                + ", got "
                                + httpResponse.statusCode()
                                + " "
                                + httpResponse.body());
            }
        } else {
            body = httpResponse.body();
        }
        try {
            return MAPPER.readValue(body, JwkKeys.class);
        } catch (JsonProcessingException ex) {
            log.warn("Failed to parse keys from URL: {}, got {}", jwksUri.uri(), body, ex);
            throw ex;
        }
    }

    private HttpResponse<String> sendGetKeysRequest(JwksUri jwksUri, boolean useToken)
            throws IOException {
        final HttpRequest.Builder builder =
                HttpRequest.newBuilder()
                        .uri(URI.create(jwksUri.uri()))
                        .version(HttpClient.Version.HTTP_1_1)
                        .GET();
        if (jwksUri.token() != null && useToken) {
            builder.header("Authorization", "Bearer " + jwksUri.token());
        }
        try {
            return httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    record JwkKeys(List<JwkKey> keys) {}

    record JwkKey(String alg, String e, String kid, String kty, String n, String use) {}

    private BigInteger base64ToBigInteger(String value) {
        return new BigInteger(1, Decoders.BASE64URL.decode(value));
    }
}
