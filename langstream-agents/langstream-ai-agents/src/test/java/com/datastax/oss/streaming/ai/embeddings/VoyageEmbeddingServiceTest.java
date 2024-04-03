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
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

// disabled, just for experiments/usage demo
@Slf4j
public class VoyageEmbeddingServiceTest {
    private String voyageApiKey = "xxxxxxxxxx"; // replace with your own API key

    @Disabled
    @Test
    public void testVoyage2() throws Exception {
        VoyageEmbeddingService.VoyageApiConfig conf =
                VoyageEmbeddingService.VoyageApiConfig.builder()
                        .accessKey(voyageApiKey)
                        .model("voyage-2")
                        .build();
        try (EmbeddingsService service = new VoyageEmbeddingService(conf)) {
            List<List<Double>> result =
                    service.computeEmbeddings(List.of("hello world", "stranger things")).get();
            result.forEach(System.out::println);
            // check the length of the result list
            assert result.size() == 2;
        }
    }

    @Disabled
    @Test
    public void testVoyageLarge2() throws Exception {
        VoyageEmbeddingService.VoyageApiConfig conf =
                VoyageEmbeddingService.VoyageApiConfig.builder()
                        .accessKey(voyageApiKey)
                        .model("voyage-large-2")
                        .build();
        try (EmbeddingsService service = new VoyageEmbeddingService(conf)) {
            List<List<Double>> result =
                    service.computeEmbeddings(List.of("hello world", "stranger things")).get();
            result.forEach(System.out::println);
            // check the length of the result list
            assert result.size() == 2;
        }
    }
}
