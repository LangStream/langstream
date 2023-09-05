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

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Disabled;

// disabled, just for experiments/usage demo
public class HuggingFaceRestEmbeddingServiceTest {

    @Disabled
    public void testMain() throws Exception {
        HuggingFaceRestEmbeddingService.HuggingFaceApiConfig conf =
                HuggingFaceRestEmbeddingService.HuggingFaceApiConfig.builder()
                        .accessKey(System.getenv("HF_API_KEY"))
                        .model("sentence-transformers/all-MiniLM-L6-v2")
                        .options(Map.of("wait_for_model", "true"))
                        .build();
        try (EmbeddingsService service = new HuggingFaceRestEmbeddingService(conf)) {
            List<List<Double>> result =
                    service.computeEmbeddings(List.of("hello world", "stranger things")).get();
            result.forEach(System.out::println);
        }
    }
}
