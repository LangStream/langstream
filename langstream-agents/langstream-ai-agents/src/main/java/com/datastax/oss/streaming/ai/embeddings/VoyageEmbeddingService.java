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
package com.datastax.oss.streaming.ai.embeddings;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * EmbeddingsService implementation using Voyage REST API.
 *
 * <p>The model requested there should be trained for "sentence similarity" task.
 */
@Slf4j
public class VoyageEmbeddingService implements EmbeddingsService {
    // https://docs.voyageai.com/reference/embeddings-api
    @Data
    @Builder
    public static class VoyageApiConfig {
        public String accessKey;

        @Builder.Default public String vgUrl = VG_URL;

        @Builder.Default public String model = "voyage-2";

        public String input_type;
        public String truncation;
        public String encoding_format;
    }

    private static final String VG_URL = "https://api.voyageai.com/v1/embeddings";

    private static final ObjectMapper om = EmbeddingsService.createObjectMapper();

    private final VoyageApiConfig conf;
    private final String model;
    private final String token;
    private final HttpClient httpClient;

    private final URI modelUrl;

    private static final int MAX_RETRIES = 3;

    @Data
    @Builder
    public static class VoyagePojo {
        @JsonAlias("input")
        public List<String> input;

        @JsonAlias("model")
        public String model;

        @JsonAlias("input_type")
        public String inputType;

        @JsonAlias("truncation")
        public Boolean truncation;

        @JsonAlias("encoding_format")
        public String encodingFormat;
    }

    public VoyageEmbeddingService(VoyageApiConfig conf) {
        this.conf = conf;
        this.model = conf.model;
        this.token = conf.accessKey;
        this.modelUrl = URI.create(conf.vgUrl);

        this.httpClient = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    }

    @Override
    public CompletableFuture<List<List<Double>>> computeEmbeddings(List<String> texts) {
        VoyagePojo.VoyagePojoBuilder pojoBuilder =
                VoyagePojo.builder().input(texts).model(this.model);

        if (this.conf.input_type != null) {
            pojoBuilder.inputType(this.conf.input_type);
        }
        if (this.conf.truncation != null) {
            pojoBuilder.truncation(Boolean.parseBoolean(this.conf.truncation));
        }
        if (this.conf.encoding_format != null) {
            pojoBuilder.encodingFormat(this.conf.encoding_format);
        }

        VoyagePojo pojo = pojoBuilder.build();
        byte[] jsonContent;
        try {
            jsonContent = om.writeValueAsBytes(pojo);
        } catch (Exception e) {
            log.error("Failed to serialize request", e);
            return CompletableFuture.failedFuture(e);
        }

        CompletableFuture<String> bodyHandle = query(jsonContent);
        return bodyHandle
                .thenApply(
                        body -> {
                            log.info("Got a query response from model {}", model);
                            try {
                                JsonNode rootNode = om.readTree(body);
                                JsonNode dataNode = rootNode.path("data");

                                List<List<Double>> embeddings = new ArrayList<>();
                                if (dataNode.isArray()) {
                                    for (JsonNode dataItem : dataNode) {
                                        JsonNode embeddingNode = dataItem.path("embedding");
                                        if (embeddingNode.isArray()) {
                                            List<Double> embedding = new ArrayList<>();
                                            for (JsonNode value : embeddingNode) {
                                                embedding.add(value.asDouble());
                                            }
                                            embeddings.add(embedding);
                                        }
                                    }
                                }
                                return embeddings;
                            } catch (Exception e) {
                                log.error("Error processing JSON", e);
                                throw new RuntimeException("Error processing JSON", e);
                            }
                        })
                .exceptionally(
                        ex -> {
                            log.error("Failed to process embeddings", ex);
                            throw new CompletionException(ex);
                        });
    }

    private CompletableFuture<String> query(byte[] jsonPayload, int attempt) {
        HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(modelUrl)
                        .header("Authorization", "Bearer " + token)
                        .POST(HttpRequest.BodyPublishers.ofByteArray(jsonPayload))
                        .build();
        return httpClient
                .sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenApply(
                        response -> {
                            if (response.statusCode() != 200) {
                                log.error(
                                        "Model {} query failed with status {}: {}",
                                        model,
                                        response.statusCode(),
                                        response.body());
                                throw new RuntimeException(
                                        "Model query failed with status " + response.statusCode());
                            }
                            return response.body();
                        })
                .exceptionally(
                        ex -> {
                            Throwable cause =
                                    (ex instanceof CompletionException) ? ex.getCause() : ex;
                            if ((cause instanceof java.net.SocketException
                                            || cause instanceof java.io.EOFException
                                            || cause instanceof java.io.IOException)
                                    && attempt < MAX_RETRIES) {
                                log.error(
                                        "Network error ({}). Retrying... Attempt: {}",
                                        cause.getClass().getSimpleName(),
                                        attempt + 1);
                                return query(jsonPayload, attempt + 1).join(); // Recursively retry
                            } else {
                                log.error("Failed to process the model query", ex);
                                log.error("Request URI: {}", request.uri());
                                log.error("Payload: {}", jsonPayload);
                                throw new RuntimeException(
                                        "Failed to process the model query", cause);
                            }
                        });
    }

    public CompletableFuture<String> query(byte[] jsonPayload) {
        return query(jsonPayload, 0);
    }
}
