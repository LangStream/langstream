package ai.langstream.ai.agents.services.impl.bedrock;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4SignerParams;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;

@Slf4j
public class BedrockClient {


    protected static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final AwsCredentials credentials;
    private final String region;
    private final SdkHttpClient client;

    public BedrockClient(AwsCredentials credentials, String region) {
        this.credentials = credentials;
        this.region = region;
        this.client = ApacheHttpClient.create();
    }


    public <T> CompletableFuture<T> invokeModel(
            BaseInvokeModelRequest request, Class<T> responseClass) {
        return invokeModel(request)
                .thenApply(s -> parseResponse(s, responseClass));
    }


    private static <T> T parseResponse(String s, Class<T> toClass) {
        try {
            return MAPPER.readValue(s, toClass);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private CompletableFuture<String> invokeModel(BaseInvokeModelRequest invokeModelRequest) {
        final SdkHttpFullRequest request = prepareRequest(invokeModelRequest);
        final HttpExecuteRequest httpExecuteRequest = HttpExecuteRequest.builder()
                .request(request)
                .contentStreamProvider(request.contentStreamProvider().get())
                .build();
        try {
            final HttpExecuteResponse response = client.prepareRequest(httpExecuteRequest).call();
            final String asString = new String(response.responseBody().get().readAllBytes(), StandardCharsets.UTF_8);

            if (!response.httpResponse().isSuccessful()) {
                return CompletableFuture.failedFuture(new RuntimeException(
                        "Failed to call invokeModel API: " + response.httpResponse().statusCode() + " " + asString));
            }
            return CompletableFuture.completedFuture(asString);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @SneakyThrows
    private SdkHttpFullRequest prepareRequest(BaseInvokeModelRequest request) {

        final String modelId = request.getModelId();
        final String host = "bedrock-runtime.%s.amazonaws.com".formatted(region);
        final String body = request.generateJsonBody();

        SdkHttpFullRequest.Builder req = SdkHttpFullRequest.builder()
                .method(SdkHttpMethod.POST);

        StringBuilder url = new StringBuilder();
        url.append("https://").append(host);
        String path = "/model/%s/invoke".formatted(modelId);
        url.append(path);
        try {
            final URI uri = new URI(url.toString());
            req.uri(uri);
            log.info("Sending request to {}", uri);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid request URI: " + url.toString());
        }
        req.putHeader("Content-Type", "application/json");
        req.contentStreamProvider(() -> new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8)));

        req.putHeader("Accept-Encoding", "gzip");

        Aws4SignerParams signerParams = Aws4SignerParams.builder()
                .awsCredentials(credentials)
                .signingName("bedrock")
                .signingRegion(Region.of(region))
                .build();
        return Aws4Signer.create().sign(req.build(), signerParams);
    }


}
