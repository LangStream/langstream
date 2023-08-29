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

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import ai.langstream.api.storage.ApplicationStore;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import io.jsonwebtoken.Jwts;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.spec.RSAPublicKeySpec;
import java.util.Base64;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest(
        properties = {
            "application.security.enabled=true",
            "application.security.token.jwks-hosts-allowlist=localhost",
            "application.security.token.auth-claim=iss",
            "application.security.token.admin-roles=testrole"
        })
@AutoConfigureMockMvc
@DirtiesContext
public class SecurityConfigurationJwksIT {

    @RegisterExtension
    WireMockExtension wm1 =
            WireMockExtension.newInstance()
                    .options(wireMockConfig().dynamicPort())
                    .failOnUnmatchedRequests(true)
                    .build();

    KeyPair kp;
    private static final String JWKS_PATH = "/auth/.well-known/jwks.json";

    @Autowired MockMvc mockMvc;

    @MockBean ApplicationStore applicationStore;

    @BeforeEach
    public void beforeEach() throws Exception {
        genAndExposeKeyPair();
    }

    @Test
    void shouldBeAuthorized() throws Exception {
        final String token =
                Jwts.builder()
                        .claim("iss", "testrole")
                        .claim("jwks_uri", wm1.url(JWKS_PATH))
                        .signWith(kp.getPrivate())
                        .compact();
        mockMvc.perform(put("/api/tenants/test").header("Authorization", "Bearer " + token))
                .andExpect(status().isOk());
    }

    @Test
    void shouldNotBeAuthorized_invalidIss() throws Exception {
        final String token =
                Jwts.builder()
                        .claim("iss", "testrole_wrong")
                        .claim("jwks_uri", wm1.url(JWKS_PATH))
                        .signWith(kp.getPrivate())
                        .compact();
        mockMvc.perform(put("/api/tenants/test").header("Authorization", "Bearer " + token))
                .andExpect(status().isForbidden());
    }

    @Test
    void shouldNotBeAuthorized_invalidKp() throws Exception {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        final String token =
                Jwts.builder()
                        .claim("iss", "testrole_wrong")
                        .claim("jwks_uri", wm1.url(JWKS_PATH))
                        .signWith(kpg.generateKeyPair().getPrivate())
                        .compact();
        mockMvc.perform(put("/api/tenants/test").header("Authorization", "Bearer " + token))
                .andExpect(status().isForbidden());
    }

    @Test
    void shouldNotBeAuthorized_invalidJwksHost() throws Exception {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        final String token =
                Jwts.builder()
                        .claim("iss", "testrole_wrong")
                        .claim("jwks_uri", wm1.url(JWKS_PATH).replace("localhost", "127.0.0.1"))
                        .signWith(kpg.generateKeyPair().getPrivate())
                        .compact();
        mockMvc.perform(put("/api/tenants/test").header("Authorization", "Bearer " + token))
                .andExpect(status().isForbidden());
    }

    private void genAndExposeKeyPair() throws Exception {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        kp = kpg.generateKeyPair();
        final RSAPublicKeySpec spec =
                KeyFactory.getInstance("RSA").getKeySpec(kp.getPublic(), RSAPublicKeySpec.class);

        final byte[] e =
                Base64.getUrlEncoder()
                        .withoutPadding()
                        .encode(spec.getPublicExponent().toByteArray());
        final byte[] mod =
                Base64.getUrlEncoder().withoutPadding().encode(spec.getModulus().toByteArray());
        wm1.stubFor(
                WireMock.get(JWKS_PATH)
                        .willReturn(
                                WireMock.okJson(
                                        """
                                  {"keys":[{"alg":"RS256","e":"%s","kid":"1","kty":"RSA","n":"%s"}]}
                                  """
                                                .formatted(
                                                        new String(e, StandardCharsets.UTF_8),
                                                        new String(mod, StandardCharsets.UTF_8)))));
    }
}
