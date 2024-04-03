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

import ai.langstream.ai.agents.services.ServiceProviderProvider;
import ai.langstream.api.runner.code.MetricsReporter;
import com.datastax.oss.streaming.ai.completions.CompletionsService;
import com.datastax.oss.streaming.ai.embeddings.EmbeddingsService;
import com.datastax.oss.streaming.ai.embeddings.VoyageEmbeddingService;
import com.datastax.oss.streaming.ai.services.ServiceProvider;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

public class VoyageProvider implements ServiceProviderProvider {

    @Override
    public boolean supports(Map<String, Object> agentConfiguration) {
        return agentConfiguration.containsKey("voyage");
    }

    @Override
    public ServiceProvider createImplementation(
            Map<String, Object> agentConfiguration, MetricsReporter metricsReporter) {
        return new VoyageServiceProvider((Map<String, Object>) agentConfiguration.get("voyage"));
    }

    @Slf4j
    static class VoyageServiceProvider implements ServiceProvider {
        private final Map<String, Object> providerConfiguration;

        public VoyageServiceProvider(Map<String, Object> providerConfiguration) {
            this.providerConfiguration = providerConfiguration;
        }

        @Override
        public CompletionsService getCompletionsService(
                Map<String, Object> additionalConfiguration) {
            throw new IllegalArgumentException("Completions service not supported");
        }

        public EmbeddingsService getEmbeddingsService(Map<String, Object> additionalConfiguration)
                throws Exception {
            String model = (String) additionalConfiguration.get("model");
            String apiUrl = (String) this.providerConfiguration.get("api-url");
            log.info(
                    "Creating Voyage embeddings service for model {} with API URL {}",
                    model,
                    apiUrl);
            VoyageEmbeddingService.VoyageApiConfig.VoyageApiConfigBuilder apiBuilder =
                    VoyageEmbeddingService.VoyageApiConfig.builder()
                            .accessKey((String) this.providerConfiguration.get("access-key"))
                            .model(model);
            if (apiUrl != null && !apiUrl.isEmpty()) {
                apiBuilder.vgUrl(apiUrl);
            }
            return new VoyageEmbeddingService(apiBuilder.build());
        }

        public void close() {}
    }
}
