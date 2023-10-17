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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datastax.oss.streaming.ai.completions.CompletionsService;
import com.datastax.oss.streaming.ai.completions.TextCompletionResult;
import com.datastax.oss.streaming.ai.embeddings.EmbeddingsService;
import com.datastax.oss.streaming.ai.services.ServiceProvider;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
class AwsBedrockProviderTest {

    protected static final String ACCESS_KEY = "..";
    protected static final String SECRET_KEY = "..";

    @Test
    void testCallEmbeddings() throws Exception {
        BedrockServiceProvider provider = new BedrockServiceProvider();
        ServiceProvider implementation =
                provider.createImplementation(
                        Map.of(
                                "bedrock",
                                Map.of(
                                        "access-key",
                                        ACCESS_KEY,
                                        "secret-key",
                                        SECRET_KEY,
                                        "region",
                                        "us-east-1")));

        EmbeddingsService embeddingsService = implementation.getEmbeddingsService(Map.of());
        List<List<Double>> result =
                embeddingsService.computeEmbeddings(List.of("hello world")).get();
        log.info("result: {}", result);
        assertEquals(1, result.size());
        assertEquals(List.of(1.d, 5.4d, 8.7d), result.get(0));
    }

    @Test
    void testCallCompletions() throws Exception {
        BedrockServiceProvider provider = new BedrockServiceProvider();
        ServiceProvider implementation =
                provider.createImplementation(
                        Map.of(
                                "bedrock",
                                Map.of(
                                        "access-key",
                                        ACCESS_KEY,
                                        "secret-key",
                                        SECRET_KEY,
                                        "region",
                                        "us-east-1")));

        final CompletionsService completionsService =
                implementation.getCompletionsService(Map.of());
        final TextCompletionResult result =
                completionsService
                        .getTextCompletions(
                                List.of("Translate to spanish: 'hello world'"),
                                null,
                                Map.of(
                                        "model",
                                        "ai21.j2-ultra-v1",
                                        "options",
                                        Map.of(
                                                "parameters",
                                                Map.of(
                                                        "maxTokens",
                                                        200,
                                                        "temperature",
                                                        0.5,
                                                        "topP",
                                                        0.5),
                                                "text-completion-expression",
                                                "${completions[0].data.text}")))
                        .get();
        log.info("result {}", result.text());
    }
}
