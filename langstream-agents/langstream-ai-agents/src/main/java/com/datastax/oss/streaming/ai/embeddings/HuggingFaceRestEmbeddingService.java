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
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * EmbeddingsService implementation using HuggingFace REST API.
 *
 * <p>The model requested there should be trained for "sentence similarity" task.
 */
@Slf4j
public class HuggingFaceRestEmbeddingService implements EmbeddingsService {

    // https://huggingface.co/docs/api-inference/detailed_parameters#feature-extraction-task
    @Data
    @Builder
    public static class HuggingFaceApiConfig {
        public String accessKey;
        public String model;

        @Builder.Default public String hfUrl = HF_URL;

        @Builder.Default public String hfCheckUrl = HF_CHECK_URL;

        @Builder.Default public Map<String, String> options = Map.of("wait_for_model", "true");
    }

    private static final String HF_URL =
            "https://api-inference.huggingface.co/pipeline/feature-extraction/";
    private static final String HF_CHECK_URL = "https://huggingface.co/api/models/";

    private static final ObjectMapper om = EmbeddingsService.createObjectMapper();

    private final HuggingFaceApiConfig conf;
    private final String model;
    private final String token;

    private final URL modelUrl;
    private final URL checkUrl;

    private final HttpClient httpClient;

    @Data
    @Builder
    public static class HuggingPojo {
        @JsonAlias("inputs")
        public List<String> inputs;

        @JsonAlias("options")
        public Map<String, String> options;
    }

    public HuggingFaceRestEmbeddingService(HuggingFaceApiConfig conf) throws MalformedURLException {
        this.conf = conf;
        this.model = conf.model;
        this.token = conf.accessKey;
        this.checkUrl = new URL(conf.hfCheckUrl + model);
        this.modelUrl = new URL(conf.hfUrl + model);

        this.httpClient = HttpClient.newHttpClient();

        try {
            HttpRequest request =
                    HttpRequest.newBuilder()
                            .uri(checkUrl.toURI())
                            .header("Authorization", "Bearer " + token)
                            .GET()
                            .build();
            HttpResponse<String> response =
                    httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (log.isDebugEnabled()) {
                log.debug(
                        "Model {} check http response is {} {}", model, response, response.body());
            }
            if (response.statusCode() != 200) {
                log.warn("Model {} check http response is {} {}", model, response, response.body());
                throw new IllegalArgumentException("Model " + model + " is not found");
            }

            Map check = om.readValue(response.body(), Map.class);
            log.info("Model {} check response is {}", model, check);
            if (check == null) {
                throw new IllegalArgumentException("Model " + model + " is not found");
            }
            if (!check.get("modelId").equals(model)) {
                throw new IllegalArgumentException("Model " + model + " is not found");
            }
            Set<String> tags = new HashSet<>((List) check.get("tags"));
            if (!tags.contains("sentence-transformers")) {
                throw new IllegalArgumentException(
                        "Model "
                                + model
                                + " is not a sentence-transformers model and not suitable for embeddings");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletableFuture<List<List<Double>>> computeEmbeddings(List<String> texts) {
        HuggingPojo pojo = HuggingPojo.builder().inputs(texts).options(conf.options).build();

        try {
            String jsonContent = om.writeValueAsString(pojo);

            CompletableFuture<String> bodyHandle = query(jsonContent);
            return bodyHandle.thenApply(
                    body -> {
                        try {
                            Object result = om.readValue(body, Object.class);
                            return (List<List<Double>>) result;
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private CompletableFuture<String> query(String jsonPayload) throws Exception {
        HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(modelUrl.toURI())
                        .header("Authorization", "Bearer " + token)
                        .POST(HttpRequest.BodyPublishers.ofString(jsonPayload))
                        .build();
        CompletableFuture<HttpResponse<String>> responseHandle =
                httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString());
        return responseHandle.thenApply(
                response -> {
                    if (log.isDebugEnabled()) {
                        log.debug(
                                "Model {} query response is {} {}",
                                model,
                                response,
                                response.body());
                    }

                    if (response.statusCode() != 200) {
                        log.warn(
                                "Model {} query failed with {} {}",
                                model,
                                response,
                                response.body());
                        throw new RuntimeException(
                                "Model "
                                        + model
                                        + " query failed with status "
                                        + response.statusCode());
                    }

                    return response.body();
                });
    }
}
