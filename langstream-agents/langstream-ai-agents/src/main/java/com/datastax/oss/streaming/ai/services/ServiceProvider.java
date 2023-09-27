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

import com.datastax.oss.streaming.ai.completions.CompletionsService;
import com.datastax.oss.streaming.ai.embeddings.EmbeddingsService;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface ServiceProvider extends AutoCloseable {
    CompletionsService getCompletionsService(Map<String, Object> additionalConfiguration)
            throws Exception;

    EmbeddingsService getEmbeddingsService(Map<String, Object> additionalConfiguration)
            throws Exception;

    void close();

    class NoopServiceProvider implements ServiceProvider {
        @Override
        public CompletionsService getCompletionsService(
                Map<String, Object> additionalConfiguration) {
            throw new IllegalArgumentException("please configure an AI service");
        }

        @Override
        public EmbeddingsService getEmbeddingsService(Map<String, Object> additionalConfiguration) {
            return new EmbeddingsService() {
                @Override
                public CompletableFuture<List<List<Double>>> computeEmbeddings(List<String> texts) {
                    return CompletableFuture.completedFuture(List.of());
                }
            };
        }

        @Override
        public void close() {}
    }
}
