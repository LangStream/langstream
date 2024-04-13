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
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
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

    private final URL modelUrl;

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

    public VoyageEmbeddingService(VoyageApiConfig conf) throws MalformedURLException {
        this.conf = conf;
        this.model = conf.model;
        this.token = conf.accessKey;
        this.modelUrl = new URL(conf.vgUrl);

        this.httpClient = HttpClient.newBuilder().build();
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
        String jsonContent;
        try {
            jsonContent = om.writeValueAsString(pojo);
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

    private CompletableFuture<String> query(String jsonPayload) {
        HttpRequest request;
        try {
            request =
                    HttpRequest.newBuilder()
                            .uri(modelUrl.toURI())
                            .header("Authorization", "Bearer " + token)
                            .POST(HttpRequest.BodyPublishers.ofString(jsonPayload))
                            .build();
        } catch (URISyntaxException e) {
            log.error("Invalid URI: {}", modelUrl, e);
            return CompletableFuture.failedFuture(e);
        }

        CompletableFuture<HttpResponse<String>> responseHandle =
                httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString());

        return responseHandle
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
                            log.error("Failed to process the model query", ex);
                            log.error("Request URI: {}", request.uri());
                            log.error("Payload: {}", jsonPayload);
                            if (ex instanceof CompletionException && ex.getCause() != null) {
                                Throwable cause = ex.getCause();
                                log.error(
                                        "Underlying exception: {} {}",
                                        cause.getClass(),
                                        cause.getMessage());
                            }
                            throw new RuntimeException("Failed to process the model query", ex);
                        });
    }
}
