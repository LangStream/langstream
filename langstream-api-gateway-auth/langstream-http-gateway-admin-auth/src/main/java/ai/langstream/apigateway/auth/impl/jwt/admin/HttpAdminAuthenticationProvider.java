package ai.langstream.apigateway.auth.impl.jwt.admin;

import ai.langstream.api.gateway.GatewayAdminAuthenticationProvider;
import ai.langstream.api.gateway.GatewayAdminRequestContext;
import ai.langstream.api.gateway.GatewayAuthenticationResult;
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
public class HttpAdminAuthenticationProvider implements GatewayAdminAuthenticationProvider {

    private static final ObjectMapper mapper = new ObjectMapper();
    private HttpAdminAuthenticationProviderConfiguration httpConfiguration;
    private HttpClient httpClient;


    @Override
    public String type() {
        return "http";
    }

    @Override
    @SneakyThrows
    public void initialize(Map<String, Object> configuration) {
        httpConfiguration =
                mapper.convertValue(configuration, HttpAdminAuthenticationProviderConfiguration.class);
        httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30))
                .followRedirects(HttpClient.Redirect.ALWAYS)
                .build();
    }

    @Override
    public GatewayAuthenticationResult authenticate(GatewayAdminRequestContext context) {

        final Map<String, String> placeholders = Map.of("tenant", context.tenant());
        final String uri = resolvePlaceholders(placeholders, httpConfiguration.getPathTemplate());
        final String url = httpConfiguration.getBaseUrl() + uri;

        log.info("Authenticating admin with url: {}", url);


        final HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url));

        httpConfiguration.getHeaders().forEach(builder::header);
        builder.header("Authorization", "Bearer " + context.credentials());
        final HttpRequest request = builder
                .GET()
                .build();

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
            return GatewayAuthenticationResult.authenticationSuccessful(
                    context.adminCredentialsInputs());
        }
        return GatewayAuthenticationResult.authenticationFailed("Http authentication failed: " + response.statusCode());
    }

    private static String resolvePlaceholders(Map<String, String> placeholders, String url) {
        for (Map.Entry<String, String> entry : placeholders.entrySet()) {
            url = url.replace("{" + entry.getKey() + "}", entry.getValue());
        }
        return url;
    }
}
