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

import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.unauthorized;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import io.jsonwebtoken.JwsHeader;
import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.Jwts;
import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.spec.RSAPublicKeySpec;
import java.util.Base64;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@WireMockTest
class LocalKubernetesOIDCTest {

    static WireMockRuntimeInfo wireMockRuntimeInfo;
    static String jwksUri;
    static KeyPair kp;

    @BeforeAll
    static void onBeforeAll(WireMockRuntimeInfo info) throws Exception {
        wireMockRuntimeInfo = info;
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        kp = kpg.generateKeyPair();
    }

    private static void genAndExposeKeyPair(String expectToken) throws Exception {

        final RSAPublicKeySpec spec =
                KeyFactory.getInstance("RSA").getKeySpec(kp.getPublic(), RSAPublicKeySpec.class);

        final byte[] e =
                Base64.getUrlEncoder()
                        .withoutPadding()
                        .encode(spec.getPublicExponent().toByteArray());
        final byte[] mod =
                Base64.getUrlEncoder().withoutPadding().encode(spec.getModulus().toByteArray());
        final String okResponse =
                """
                {"keys":[{"alg":"RS256","e":"to-ignore","kid":"1","kty":"RSA","n":"ignore-me"}, {"alg":"RS256","e":"%s","kid":"2","kty":"RSA","n":"%s"}]}
                """
                        .formatted(
                                new String(e, StandardCharsets.UTF_8),
                                new String(mod, StandardCharsets.UTF_8));

        if (expectToken != null) {
            wireMockRuntimeInfo
                    .getWireMock()
                    .register(
                            WireMock.get("/openid/v1/jwks")
                                    .withHeader("Authorization", equalTo("Bearer " + expectToken))
                                    .willReturn(WireMock.ok(okResponse)));

        } else {
            wireMockRuntimeInfo
                    .getWireMock()
                    .register(WireMock.get("/openid/v1/jwks").willReturn(WireMock.ok(okResponse)));
        }
        jwksUri = wireMockRuntimeInfo.getHttpBaseUrl() + "/openid/v1/jwks";
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testKubernetesOIDC(boolean oidcRequiresToken) throws Exception {
        final String localTokenPath;
        if (oidcRequiresToken) {
            stubFor(get(anyUrl()).willReturn(unauthorized()));

            final String localToken =
                    Jwts.builder()
                            .claim("iss", wireMockRuntimeInfo.getHttpBaseUrl())
                            .signWith(kp.getPrivate())
                            .compact();

            final Path tokenFile = Files.createTempFile("test", "");
            Files.writeString(tokenFile, localToken, StandardCharsets.UTF_8);

            genAndExposeKeyPair(localToken);
            localTokenPath = tokenFile.toAbsolutePath().toString();

            wireMockRuntimeInfo
                    .getWireMock()
                    .register(
                            WireMock.get("/.well-known/openid-configuration")
                                    .withHeader(
                                            "Authorization",
                                            equalTo("Bearer %s".formatted(localToken)))
                                    .willReturn(
                                            WireMock.ok(
                                                    "{\"issuer\":\"%s\","
                                                                    .formatted(
                                                                            wireMockRuntimeInfo
                                                                                    .getHttpBaseUrl())
                                                            + "\"jwks_uri\":\"%s\","
                                                                    .formatted(jwksUri)
                                                            + "\"response_types_supported\":[\"id_token\"],"
                                                            + "\"subject_types_supported\":[\"public\"],"
                                                            + "\"id_token_signing_alg_values_supported\":[\"RS256\"]}")));
        } else {
            localTokenPath = null;
            genAndExposeKeyPair(null);
            wireMockRuntimeInfo
                    .getWireMock()
                    .register(
                            WireMock.get("/.well-known/openid-configuration")
                                    .willReturn(
                                            WireMock.ok(
                                                    "{\"issuer\":\"%s\","
                                                                    .formatted(
                                                                            wireMockRuntimeInfo
                                                                                    .getHttpBaseUrl())
                                                            + "\"jwks_uri\":\"%s\","
                                                                    .formatted(jwksUri)
                                                            + "\"response_types_supported\":[\"id_token\"],"
                                                            + "\"subject_types_supported\":[\"public\"],"
                                                            + "\"id_token_signing_alg_values_supported\":[\"RS256\"]}")));
        }

        final JwtProperties props =
                new JwtProperties(null, null, "sub", null, null, null, null, true, "ls-");
        AuthenticationProviderToken authenticationProviderToken =
                getProvider(props, localTokenPath);
        assertAuthenticated(
                authenticationProviderToken,
                baseToken()
                        .setHeader(Map.of(JwsHeader.KEY_ID, "2"))
                        .claim("kubernetes.io", Map.of("namespace", "ls-t1"))
                        .compact(),
                "t1");

        assertAuthFailed(
                authenticationProviderToken,
                baseToken()
                        .setHeader(Map.of(JwsHeader.KEY_ID, "2"))
                        .claim("kubernetes.io", Map.of("namespace", "else-ls-t1"))
                        .compact(),
                "Token was valid, however no principal found.");

        assertAuthFailed(
                authenticationProviderToken,
                baseToken()
                        .setHeader(Map.of(JwsHeader.KEY_ID, "1"))
                        .claim("kubernetes.io", Map.of("namespace", "ls-t1"))
                        .compact(),
                "Failed to authentication token: Failed to parse public key '1' from "
                        + wireMockRuntimeInfo.getHttpBaseUrl()
                        + "/openid/v1/jwks");

        assertAuthFailed(
                authenticationProviderToken,
                baseToken().claim("kubernetes.io", Map.of("namespace", "ls-t1")).compact(),
                "Failed to authentication token: Failed to parse public key '1' from "
                        + wireMockRuntimeInfo.getHttpBaseUrl()
                        + "/openid/v1/jwks");
    }

    private JwtBuilder baseToken() {
        return Jwts.builder()
                .claim("iss", wireMockRuntimeInfo.getHttpBaseUrl())
                .signWith(kp.getPrivate());
    }

    private static void assertAuthenticated(
            AuthenticationProviderToken provider, String token, String expectedPrincipal)
            throws AuthenticationProviderToken.AuthenticationException {
        assertEquals(expectedPrincipal, provider.authenticate(token));
    }

    private static void assertAuthFailed(
            AuthenticationProviderToken provider, String token, String message)
            throws AuthenticationProviderToken.AuthenticationException {
        try {
            provider.authenticate(token);
            fail();
        } catch (AuthenticationProviderToken.AuthenticationException ex) {
            assertEquals(message, ex.getMessage());
            return;
        }
    }

    private AuthenticationProviderToken getProvider(JwtProperties props, String localTokenPath)
            throws IOException {

        final LocalKubernetesJwksUriSigningKeyResolver localk8sResolver =
                new LocalKubernetesJwksUriSigningKeyResolver(
                        HttpClient.newHttpClient(),
                        localTokenPath,
                        wireMockRuntimeInfo.getHttpBaseUrl());
        AuthenticationProviderToken authenticationProviderToken =
                new AuthenticationProviderToken(
                        props,
                        new JwksUriSigningKeyResolver("RS256", null, null, localk8sResolver));
        return authenticationProviderToken;
    }
}
