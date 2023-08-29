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

import com.azure.ai.openai.OpenAIClient;
import com.azure.ai.openai.models.Embeddings;
import com.azure.ai.openai.models.EmbeddingsOptions;
import java.util.List;
import java.util.stream.Collectors;

public class OpenAIEmbeddingsService implements EmbeddingsService {

    private final OpenAIClient openAIClient;
    private final String model;

    public OpenAIEmbeddingsService(OpenAIClient openAIClient, String model) {
        this.openAIClient = openAIClient;
        this.model = model;
    }

    @Override
    public List<List<Double>> computeEmbeddings(List<String> texts) {
        EmbeddingsOptions embeddingsOptions = new EmbeddingsOptions(texts);
        Embeddings embeddings = openAIClient.getEmbeddings(model, embeddingsOptions);
        return embeddings.getData().stream()
                .map(embedding -> embedding.getEmbedding())
                .collect(Collectors.toList());
    }
}
