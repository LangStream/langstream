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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class HuggingFaceEmbeddingServiceTest {

    @Test
    public void testEmbeddings() throws Exception {
        AbstractHuggingFaceEmbeddingService.HuggingFaceConfig conf =
                AbstractHuggingFaceEmbeddingService.HuggingFaceConfig.builder()
                        .engine("PyTorch")
                        .modelName("multilingual-e5-small")
                        .modelUrl("djl://ai.djl.huggingface.pytorch/intfloat/multilingual-e5-small")
                        .build();

        try (EmbeddingsService service = new HuggingFaceEmbeddingService(conf)) {

            List<List<Double>> lists =
                    service.computeEmbeddings(List.of("Hello", "my friend")).get();
            assertEquals(2, lists.size());
            assertEquals(List.of(384), List.of(lists.get(0).size()));
            assertEquals(List.of(384), List.of(lists.get(1).size()));
        }
    }
}
