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
package ai.langstream.impl.agents.ai.steps;

import ai.langstream.api.doc.AgentConfig;
import ai.langstream.api.doc.ConfigProperty;
import ai.langstream.api.model.AgentConfiguration;
import ai.langstream.impl.agents.ai.GenAIToolKitFunctionAgentProvider;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import lombok.Data;

@AgentConfig(
        name = "Compute embeddings of the record",
        description =
                """
                        Compute embeddings of the record. The embeddings are stored in the record under a specific field.
                        """)
@Data
public class ComputeAIEmbeddingsConfiguration extends BaseGenAIStepConfiguration {
    public static final GenAIToolKitFunctionAgentProvider.StepConfigurationInitializer STEP =
            new GenAIToolKitFunctionAgentProvider.StepConfigurationInitializer() {
                @Override
                public Class getAgentConfigurationModelClass() {
                    return ComputeAIEmbeddingsConfiguration.class;
                }

                @Override
                public void generateSteps(
                        Map<String, Object> step,
                        Map<String, Object> originalConfiguration,
                        AgentConfiguration agentConfiguration,
                        GenAIToolKitFunctionAgentProvider.DataSourceConfigurationGenerator
                                dataSourceConfigurationGenerator,
                        GenAIToolKitFunctionAgentProvider.TopicConfigurationGenerator
                                topicConfigurationGenerator,
                        GenAIToolKitFunctionAgentProvider.AIServiceConfigurationGenerator
                                aiServiceConfigurationGenerator) {
                    GenAIToolKitFunctionAgentProvider.StepConfigurationInitializer.super
                            .generateSteps(
                                    step,
                                    originalConfiguration,
                                    agentConfiguration,
                                    dataSourceConfigurationGenerator,
                                    topicConfigurationGenerator,
                                    aiServiceConfigurationGenerator);
                    aiServiceConfigurationGenerator.generateAIServiceConfiguration(
                            (String) step.remove("ai-service"));
                }
            };

    @ConfigProperty(
            description =
                    """
                            Model to use for the embeddings. The model must be available in the configured AI Service.
                            """,
            defaultValue = "text-embedding-ada-002")
    private String model = "text-embedding-ada-002";

    @ConfigProperty(
            description =
                    """
                            Text to create embeddings from. You can use Mustache syntax to compose multiple fields into a single text. Example:
                            text: "{{{ value.field1 }}} {{{ value.field2 }}}"
                            """,
            required = true)
    private String text;

    @ConfigProperty(
            description =
                    """
                            Field where to store the embeddings.
                            """,
            required = true)
    @JsonProperty("embeddings-field")
    private String embeddingsField;

    @ConfigProperty(
            description =
                    """
                    Execute the agent over a list of documents
                    """)
    @JsonProperty("loop-over")
    private String loopOver;

    @ConfigProperty(
            description =
                    """
                            Batch size for submitting the embeddings requests.
                            """,
            defaultValue = "10")
    @JsonProperty("batch-size")
    private int batchSize = 10;

    @ConfigProperty(
            description =
                    """
                            Max number of concurrent requests to the AI Service.
                            """,
            defaultValue = "4")
    private int concurrency = 4;

    @ConfigProperty(
            description =
                    """
                            Flushing is disabled by default in order to avoid latency spikes.
                            You should enable this feature in the case of background processing.""",
            defaultValue = "0")
    @JsonProperty("flush-interval")
    private int flushInterval;

    @ConfigProperty(
            description =
                    """
                            In case of multiple AI services configured, specify the id of the AI service to use.
                            """)
    @JsonProperty(value = "ai-service")
    private String aiService;

    @ConfigProperty(
            description =
                    """
                            Additional options to pass to the AI Service. (HuggingFace only)
                            """)
    private Map<String, String> options;

    @ConfigProperty(
            description =
                    """
                            Additional arguments to pass to the AI Service. (HuggingFace only)
                            """)
    private Map<String, String> arguments;

    @ConfigProperty(
            description =
                    """
                            URL of the model to use. (HuggingFace only). The default is computed from the model: "djl://ai.djl.huggingface.pytorch/{model}"
                             """)
    @JsonProperty("model-url")
    private String modelUrl;
}
