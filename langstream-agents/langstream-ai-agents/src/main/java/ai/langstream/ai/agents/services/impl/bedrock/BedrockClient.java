package ai.langstream.ai.agents.services.impl.bedrock;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.zip.GZIPInputStream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hc.client5.http.entity.GzipDecompressingEntity;
import org.apache.hc.core5.http.HttpEntity;
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
public class BedrockClient implements AutoCloseable {

    protected static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final AwsCredentials credentials;
    private final String url;
    private final String signingRegion;
    private final SdkHttpClient client;

    public BedrockClient(AwsCredentials credentials, String url, String signingRegion) {
        this.credentials = credentials;
        this.signingRegion = signingRegion;
        this.url = url;
        this.client = ApacheHttpClient.create();
    }


    public <T> CompletableFuture<T> invokeModel(
            BaseInvokeModelRequest request, Class<T> responseClass) {
        return invokeModel(request)
                .thenApply(s -> parseResponse(s, responseClass));
    }


    private static <T> T parseResponse(InputStream in, Class<T> toClass) {
        try {
            return MAPPER.readValue(in, toClass);
        } catch (Exception e) {
            try {
                log.error("Failed to parse response: {}", new String(in.readAllBytes(), StandardCharsets.UTF_8), e);
            } catch (IOException ioException) {
                log.error("Failed to parse response", e);
            }
            throw new RuntimeException(e);
        }
    }


    private CompletableFuture<InputStream> invokeModel(BaseInvokeModelRequest invokeModelRequest) {
        final SdkHttpFullRequest request = prepareRequest(invokeModelRequest);
        final HttpExecuteRequest httpExecuteRequest = HttpExecuteRequest.builder()
                .request(request)
                .contentStreamProvider(request.contentStreamProvider().get())
                .build();
        try {
            final HttpExecuteResponse response = client.prepareRequest(httpExecuteRequest).call();
            final InputStream body = parseResponseBody(response);
            if (!response.httpResponse().isSuccessful()) {
                return CompletableFuture.failedFuture(new RuntimeException(
                        "Failed to call invokeModel API: " + response.httpResponse().statusCode() + " " + new String(body.readAllBytes(), StandardCharsets.UTF_8)));
            }
            return CompletableFuture.completedFuture(body);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private InputStream parseResponseBody(HttpExecuteResponse response) throws IOException {
        final byte[] bytes = response.responseBody().get().readAllBytes();
        InputStream bodyStream = new ByteArrayInputStream(bytes);
        boolean isZipped = response.httpResponse().firstMatchingHeader("Content-Encoding")
                .map(enc -> enc.contains("gzip"))
                .orElse(Boolean.FALSE);
        if (bodyStream != null && isZipped) {
            bodyStream = new GZIPInputStream(bodyStream);
        }
        return bodyStream;
    }

    @SneakyThrows
    private SdkHttpFullRequest prepareRequest(BaseInvokeModelRequest request) {

        final String modelId = request.getModelId();
        final String body = request.generateJsonBody();

        SdkHttpFullRequest.Builder req = SdkHttpFullRequest.builder()
                .method(SdkHttpMethod.POST);

        String baseUrl = url.startsWith("https://") || url.startsWith("http://") ? url : "https://" + url;
        if (baseUrl.endsWith("/")) {
            baseUrl = baseUrl.substring(0, baseUrl.length() - 1);
        }
        final String finalUrl = "%s/model/%s/invoke".formatted(baseUrl, modelId);

        try {
            final URI uri = new URI(finalUrl);
            req.uri(uri);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid request URI: " + finalUrl);
        }
        req.putHeader("Content-Type", "application/json");
        req.contentStreamProvider(() -> new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8)));

        req.putHeader("Accept-Encoding", "gzip");

        Aws4SignerParams signerParams = Aws4SignerParams.builder()
                .awsCredentials(credentials)
                .signingName("bedrock")
                .signingRegion(Region.of(signingRegion))
                .build();
        return Aws4Signer.create().sign(req.build(), signerParams);
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
        }
    }
}
