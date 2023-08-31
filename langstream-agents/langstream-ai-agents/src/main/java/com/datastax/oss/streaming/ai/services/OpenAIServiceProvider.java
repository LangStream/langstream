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
package com.datastax.oss.streaming.ai.services;

import ai.langstream.ai.agents.services.impl.OpenAICompletionService;
import com.azure.ai.openai.OpenAIClient;
import com.datastax.oss.streaming.ai.completions.CompletionsService;
import com.datastax.oss.streaming.ai.embeddings.EmbeddingsService;
import com.datastax.oss.streaming.ai.embeddings.OpenAIEmbeddingsService;
import com.datastax.oss.streaming.ai.model.config.TransformStepConfig;
import com.datastax.oss.streaming.ai.util.TransformFunctionUtil;
import java.util.Map;

public class OpenAIServiceProvider implements ServiceProvider {

    private final OpenAIClient client;

    public OpenAIServiceProvider(TransformStepConfig config) {
        client = TransformFunctionUtil.buildOpenAIClient(config.getOpenai());
    }

    public OpenAIServiceProvider(OpenAIClient client) {
        this.client = client;
    }

    @Override
    public CompletionsService getCompletionsService(Map<String, Object> additionalConfiguration) {
        return new OpenAICompletionService(client);
    }

    @Override
    public EmbeddingsService getEmbeddingsService(Map<String, Object> additionalConfiguration) {
        String model = (String) additionalConfiguration.get("model");
        return new OpenAIEmbeddingsService(client, model);
    }

    @Override
    public void close() {}
}
