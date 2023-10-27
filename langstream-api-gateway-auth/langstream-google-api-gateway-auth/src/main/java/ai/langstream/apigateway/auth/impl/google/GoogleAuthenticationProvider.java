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
package ai.langstream.apigateway.auth.impl.google;

import ai.langstream.api.gateway.GatewayAuthenticationProvider;
import ai.langstream.api.gateway.GatewayAuthenticationResult;
import ai.langstream.api.gateway.GatewayRequestContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken;
import com.google.api.client.googleapis.auth.oauth2.GoogleIdTokenVerifier;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GoogleAuthenticationProvider implements GatewayAuthenticationProvider {

    protected static final String FIELD_SUBJECT = "subject";
    protected static final String FIELD_EMAIL = "email";
    protected static final String FIELD_NAME = "name";
    protected static final String FIELD_LOCALE = "locale";
    private static final ObjectMapper mapper = new ObjectMapper();

    private GoogleIdTokenVerifier verifier;

    @Override
    public String type() {
        return "google";
    }

    @Override
    public void initialize(Map<String, Object> configuration) {
        final GoogleAuthenticationProviderConfiguration config =
                mapper.convertValue(configuration, GoogleAuthenticationProviderConfiguration.class);
        final String clientId = config.getClientId();
        if (clientId == null || clientId.isBlank()) {
            throw new IllegalArgumentException("clientId is required for Google Authentication.");
        }
        verifier =
                new GoogleIdTokenVerifier.Builder(new NetHttpTransport(), new GsonFactory())
                        .setAudience(List.of(clientId))
                        .build();
    }

    @Override
    public GatewayAuthenticationResult authenticate(GatewayRequestContext context) {
        try {
            final String credentials = context.credentials();
            if (credentials == null) {
                return GatewayAuthenticationResult.authenticationFailed(
                        "Credentials not provided.");
            }
            GoogleIdToken idToken = verifier.verify(credentials);
            if (idToken != null) {
                final GoogleIdToken.Payload payload = idToken.getPayload();
                Map<String, String> result = new HashMap<>();
                result.put(FIELD_SUBJECT, payload.getSubject());
                result.put(FIELD_EMAIL, payload.getEmail());
                result.put(FIELD_NAME, (String) payload.get("name"));
                result.put(FIELD_LOCALE, (String) payload.get("locale"));
                return GatewayAuthenticationResult.authenticationSuccessful(result);
            } else {
                return GatewayAuthenticationResult.authenticationFailed("Invalid credentials.");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
