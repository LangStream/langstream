package com.datastax.oss.sga.apigateway.auth.impl.google;

import com.datastax.oss.sga.api.gateway.GatewayAuthenticationProvider;
import com.datastax.oss.sga.api.gateway.GatewayAuthenticationResult;
import com.datastax.oss.sga.api.gateway.GatewayRequestContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken;
import com.google.api.client.googleapis.auth.oauth2.GoogleIdTokenVerifier;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import java.util.Collections;
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
        System.out.println("initialing google auth" + clientId);
        if (clientId == null || clientId.isBlank()) {
            throw new IllegalArgumentException("clientId is required for Google Authentication.");
        }
        verifier = new GoogleIdTokenVerifier.Builder(new NetHttpTransport(), new GsonFactory())
                .setAudience(List.of(clientId))
                .build();
    }

    @Override
    public GatewayAuthenticationResult authenticate(GatewayRequestContext context) {
        try {
            System.out.println("verify google auth" + context.credentials());
            GoogleIdToken idToken = verifier.verify(context.credentials());
            if (idToken != null) {
                final GoogleIdToken.Payload payload = idToken.getPayload();
                Map<String, String> result = new HashMap<>();
                result.put(FIELD_SUBJECT, payload.getSubject());
                result.put(FIELD_EMAIL, payload.getEmail());
                result.put(FIELD_NAME, (String) payload.get("name"));
                result.put(FIELD_LOCALE, (String) payload.get("locale"));
                return GatewayAuthenticationResult.authenticationSuccessful(result);
            } else {
                System.out.println("fallito...");
                return GatewayAuthenticationResult.authenticationFailed("Invalid token.");
            }
        } catch (Exception e) {
            System.out.println("ex" + e);
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
