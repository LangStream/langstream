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
package ai.langstream.ai.agents.services.impl.bedrock;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClientBuilder;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelRequest;

@Slf4j
public class BedrockClient implements AutoCloseable {

    protected static final ObjectMapper MAPPER =
            new ObjectMapper()
                    .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final BedrockRuntimeClient client;
    private final ExecutorService executorService;

    @SneakyThrows
    public BedrockClient(AwsCredentials credentials, String region, String endpointOverride) {
        final BedrockRuntimeClientBuilder builder =
                BedrockRuntimeClient.builder()
                        .credentialsProvider(() -> credentials)
                        .region(Region.of(region));
        if (endpointOverride != null) {
            builder.endpointOverride(new URI(endpointOverride));
        }
        this.client = builder.build();
        this.executorService = Executors.newCachedThreadPool();
    }

    public <T> CompletableFuture<T> invokeModel(
            BaseInvokeModelRequest request, Class<T> responseClass) {
        return invokeModel(request).thenApply(s -> parseResponse(s, responseClass));
    }

    private static <T> T parseResponse(InputStream in, Class<T> toClass) {
        try {
            return MAPPER.readValue(in, toClass);
        } catch (Exception e) {
            try {
                log.error(
                        "Failed to parse response: {}",
                        new String(in.readAllBytes(), StandardCharsets.UTF_8),
                        e);
            } catch (IOException ioException) {
                log.error("Failed to parse response", e);
            }
            throw new RuntimeException(e);
        }
    }

    private CompletableFuture<InputStream> invokeModel(BaseInvokeModelRequest invokeModelRequest) {
        return CompletableFuture.supplyAsync(
                () -> {
                    final InvokeModelRequest request =
                            InvokeModelRequest.builder()
                                    .modelId(invokeModelRequest.getModelId())
                                    .body(
                                            SdkBytes.fromUtf8String(
                                                    invokeModelRequest.generateJsonBody()))
                                    .build();
                    return client.invokeModel(request).body().asInputStream();
                },
                executorService);
    }

    @Override
    public void close() {
        if (executorService != null) {
            executorService.shutdown();
        }
        if (client != null) {
            client.close();
        }
    }
}
