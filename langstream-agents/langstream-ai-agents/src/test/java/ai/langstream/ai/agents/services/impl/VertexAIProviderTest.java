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
package ai.langstream.ai.agents.services.impl;

import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.junit.jupiter.api.Assertions.*;

import ai.langstream.api.runner.code.MetricsReporter;
import com.datastax.oss.streaming.ai.completions.ChatCompletions;
import com.datastax.oss.streaming.ai.completions.ChatMessage;
import com.datastax.oss.streaming.ai.completions.CompletionsService;
import com.datastax.oss.streaming.ai.embeddings.EmbeddingsService;
import com.datastax.oss.streaming.ai.services.ServiceProvider;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Slf4j
@WireMockTest
class VertexAIProviderTest {

    @Test
    void testCallEmbeddings(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        stubFor(
                post("/v1/projects/the-project/locations/us-central1/publishers/google/models/textembedding-gecko"
                                + ":predict")
                        .willReturn(
                                okJson(
                                        """
                                                   {
                                                      "predictions": [
                                                        {
                                                          "embeddings": {
                                                            "statistics": {
                                                              "truncated": false,
                                                              "token_count": 6
                                                            },
                                                            "values": [ 1.0, 5.4, 8.7]
                                                          }
                                                        }
                                                      ]
                                                    }
                                                """)));

        VertexAIProvider provider = new VertexAIProvider();
        ServiceProvider implementation =
                provider.createImplementation(
                        Map.of(
                                "vertex",
                                Map.of(
                                        "url",
                                        wmRuntimeInfo.getHttpBaseUrl(),
                                        "project",
                                        "the-project",
                                        "region",
                                        "us-central1",
                                        "token",
                                        "xxxx")),
                        MetricsReporter.DISABLED);

        EmbeddingsService embeddingsService =
                implementation.getEmbeddingsService(Map.of("model", "textembedding-gecko"));
        List<List<Double>> result =
                embeddingsService.computeEmbeddings(List.of("hello world")).get();
        log.info("result: {}", result);
        assertEquals(1, result.size());
        assertEquals(List.of(1.d, 5.4d, 8.7d), result.get(0));
    }

    @Test
    void testCallCompletion(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        stubFor(
                post("/v1/projects/the-project/locations/us-central1/publishers/google/models/chat-bison:predict")
                        .willReturn(
                                okJson(
                                        """
                                                   {
                                                     "predictions": [
                                                       {
                                                         "safetyAttributes": [
                                                           {
                                                             "blocked": false,
                                                             "scores": [],
                                                             "categories": []
                                                           }
                                                         ],
                                                         "citationMetadata": [
                                                           {
                                                             "citations": []
                                                           }
                                                         ],
                                                         "candidates": [
                                                           {
                                                             "author": "1",
                                                             "content": "A car is a wheeled, self-propelled motor vehicle used for transportation."
                                                           }
                                                         ]
                                                       }
                                                     ],
                                                     "metadata": {
                                                       "tokenMetadata": {
                                                         "inputTokenCount": {
                                                           "totalTokens": 5,
                                                           "totalBillableCharacters": 11
                                                         },
                                                         "outputTokenCount": {
                                                           "totalBillableCharacters": 63,
                                                           "totalTokens": 15
                                                         }
                                                       }
                                                     }
                                                   }
                                                """)));

        VertexAIProvider provider = new VertexAIProvider();
        ServiceProvider implementation =
                provider.createImplementation(
                        Map.of(
                                "vertex",
                                Map.of(
                                        "url",
                                        wmRuntimeInfo.getHttpBaseUrl(),
                                        "project",
                                        "the-project",
                                        "region",
                                        "us-central1",
                                        "token",
                                        "xxxx")),
                        MetricsReporter.DISABLED);
        CompletionsService service =
                implementation.getCompletionsService(
                        Map.of(
                                "model",
                                "chat-bison",
                                "max-tokens",
                                3,
                                "temperature",
                                0.5,
                                "top-p",
                                1.0,
                                "top-k",
                                3.0));
        ChatCompletions chatCompletions =
                service.getChatCompletions(
                                List.of(new ChatMessage("user").setContent("What is a car?")),
                                null,
                                Map.of("max_tokens", 3))
                        .get();
        log.info("result: {}", chatCompletions);
        assertEquals(
                "A car is a wheeled, self-propelled motor vehicle used for transportation.",
                chatCompletions.getChoices().get(0).getMessage().getContent());
    }

    @Test
    void testCallTextCompletion(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        stubFor(
                post("/v1/projects/the-project/locations/us-central1/publishers/google/models/text-bison:predict")
                        .willReturn(
                                okJson(
                                        """
                                                   {
                                                                                     "predictions": [
                                                                                       {
                                                                                         "citationMetadata": {
                                                                                           "citations": []
                                                                                         },
                                                                                         "safetyAttributes": {
                                                                                           "scores": [
                                                                                             0.1
                                                                                           ],
                                                                                           "categories": [
                                                                                             "Finance"
                                                                                           ],
                                                                                           "blocked": false
                                                                                         },
                                                                                         "content": "A car is a wheeled, self-propelled motor vehicle used for transportation."
                                                                                       }
                                                                                     ]
                                                                                   }
                                                """)));

        VertexAIProvider provider = new VertexAIProvider();
        ServiceProvider implementation =
                provider.createImplementation(
                        Map.of(
                                "vertex",
                                Map.of(
                                        "url",
                                        wmRuntimeInfo.getHttpBaseUrl(),
                                        "project",
                                        "the-project",
                                        "region",
                                        "us-central1",
                                        "token",
                                        "xxxx")),
                        MetricsReporter.DISABLED);
        CompletionsService service =
                implementation.getCompletionsService(
                        Map.of(
                                "model",
                                "text-bison",
                                "max-tokens",
                                3,
                                "temperature",
                                0.5,
                                "top-p",
                                1.0,
                                "top-k",
                                3.0));
        String textCompletions =
                service.getTextCompletions(List.of("explain a car"), null, Map.of("max_tokens", 3))
                        .get()
                        .text();
        log.info("result: {}", textCompletions);
        assertEquals(
                "A car is a wheeled, self-propelled motor vehicle used for transportation.",
                textCompletions);
    }

    @Test
    @Disabled
    void testCallEmbeddingsUsingServiceAccount() throws Exception {
        VertexAIProvider provider = new VertexAIProvider();
        ServiceProvider implementation =
                provider.createImplementation(
                        Map.of(
                                "vertex",
                                Map.of(
                                        "url",
                                        "https://us-central1-aiplatform.googleapis.com",
                                        "project",
                                        "datastax-astrastreaming-dev",
                                        "region",
                                        "us-central1",
                                        "serviceAccountJson",
                                        "PUT-HERE-YOUR-JSON-KEY")),
                        MetricsReporter.DISABLED);

        EmbeddingsService embeddingsService =
                implementation.getEmbeddingsService(Map.of("model", "textembedding-gecko"));
        List<List<Double>> result =
                embeddingsService.computeEmbeddings(List.of("hello world")).get();
        log.info("result: {}", result);
        assertEquals(1, result.size());
    }
}
