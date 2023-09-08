package ai.langstream.apigateway.auth.impl.jwt.admin;

import ai.langstream.api.gateway.GatewayAdminAuthenticationProvider;
import ai.langstream.api.gateway.GatewayAdminRequestContext;
import ai.langstream.api.gateway.GatewayAuthenticationResult;
import ai.langstream.auth.jwt.AuthenticationProviderToken;
import ai.langstream.auth.jwt.JwtProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;

public class JwtAdminAuthenticationProvider implements GatewayAdminAuthenticationProvider {

    private static final ObjectMapper mapper = new ObjectMapper();
    private AuthenticationProviderToken authenticationProviderToken;
    private List<String> adminRoles;

    @Override
    public String type() {
        return "jwt";
    }

    @Override
    @SneakyThrows
    public void initialize(Map<String, Object> configuration) {
        final JwtAdminAuthenticationProviderConfiguration tokenProperties =
                mapper.convertValue(configuration, JwtAdminAuthenticationProviderConfiguration.class);


        if (tokenProperties.adminRoles() != null) {
            this.adminRoles = tokenProperties.adminRoles();
        } else {
            this.adminRoles = List.of();
        }


        final JwtProperties jwtProperties = new JwtProperties(
                tokenProperties.secretKey(),
                tokenProperties.publicKey(),
                tokenProperties.authClaim(),
                tokenProperties.publicAlg(),
                tokenProperties.audienceClaim(),
                tokenProperties.audience(),
                tokenProperties.jwksHostsAllowlist());
        this.authenticationProviderToken = new AuthenticationProviderToken(jwtProperties);
    }

    @Override
    public GatewayAuthenticationResult authenticate(GatewayAdminRequestContext context) {
        String role;
        try {
            role = authenticationProviderToken.authenticate(context.credentials());
        } catch (AuthenticationProviderToken.AuthenticationException ex) {
            return GatewayAuthenticationResult.authenticationFailed(ex.getMessage());
        }
        if (!adminRoles.contains(role)) {
            return GatewayAuthenticationResult.authenticationFailed("Not an admin.");
        }
        return GatewayAuthenticationResult.authenticationSuccessful(
                context.adminCredentialsInputs());
    }
}
