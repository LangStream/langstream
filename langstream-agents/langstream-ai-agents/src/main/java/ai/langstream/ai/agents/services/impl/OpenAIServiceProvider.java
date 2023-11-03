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
import com.azure.ai.openai.OpenAIAsyncClient;
import com.datastax.oss.streaming.ai.model.config.OpenAIConfig;
import com.datastax.oss.streaming.ai.services.ServiceProvider;
import com.datastax.oss.streaming.ai.util.TransformFunctionUtil;
import java.util.Map;

public class OpenAIServiceProvider implements ServiceProviderProvider {
    @Override
    public boolean supports(Map<String, Object> agentConfiguration) {
        return agentConfiguration.containsKey("openai");
    }

    @Override
    public ServiceProvider createImplementation(
            Map<String, Object> agentConfiguration, MetricsReporter metricsReporter) {
        OpenAIConfig config =
                TransformFunctionUtil.convertFromMap(
                        (Map<String, Object>) agentConfiguration.get("openai"), OpenAIConfig.class);
        OpenAIAsyncClient client = TransformFunctionUtil.buildOpenAsyncAIClient(config);
        return new com.datastax.oss.streaming.ai.services.OpenAIServiceProvider(
                client, metricsReporter);
    }
}
