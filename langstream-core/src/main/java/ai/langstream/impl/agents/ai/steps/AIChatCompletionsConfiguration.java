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
import java.util.List;
import java.util.Map;
import lombok.Data;

@AgentConfig(
        name = "Compute chat completions",
        description =
                """
                        Sends the messages to the AI Service to compute chat completions. The result is stored in the specified field.
                        """)
@Data
public class AIChatCompletionsConfiguration extends BaseGenAIStepConfiguration {
    public static final GenAIToolKitFunctionAgentProvider.StepConfigurationInitializer STEP =
            new GenAIToolKitFunctionAgentProvider.StepConfigurationInitializer() {
                @Override
                public Class getAgentConfigurationModelClass() {
                    return AIChatCompletionsConfiguration.class;
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

                    final String streamTopic = (String) step.get("stream-to-topic");
                    if (streamTopic != null) {
                        topicConfigurationGenerator.generateTopicConfiguration(streamTopic);
                    }

                    aiServiceConfigurationGenerator.generateAIServiceConfiguration(
                            (String) step.remove("ai-service"));
                }
            };

    @Data
    public static class ChatMessage {
        @ConfigProperty(
                description =
                        """
                                Role of the message. The role is used to identify the speaker in the chat.
                                """,
                required = false)
        private String role;

        @ConfigProperty(
                description =
                        """
                                Content of the message. You can use the Mustache syntax.
                                """,
                required = true)
        private String content;
    }

    @ConfigProperty(
            description =
                    """
                            The model to use for chat completions. The model must be available in the AI Service.
                            """,
            required = true)
    private String model;

    @ConfigProperty(
            description =
                    """
                            Messages to use for chat completions. You can use the Mustache syntax.
                            """,
            required = true)
    private List<ChatMessage> messages;

    @ConfigProperty(
            description =
                    """
                            Enable streaming of the results. If enabled, the results are streamed to the specified topic in small chunks. The entire messages will be sent to the output topic instead.
                            """)
    @JsonProperty(value = "stream-to-topic")
    private String streamToTopic;

    @ConfigProperty(
            description =
                    """
                            Field to use to store the completion results in the stream-to-topic topic. Use "value" to write the result without a structured schema. Use "value.<field>" to write the result in a specific field.
                            """)
    @JsonProperty(value = "stream-response-completion-field")
    private String streamResponseCompletionField;

    @ConfigProperty(
            description =
                    """
                            Minimum number of chunks to send to the stream-to-topic topic. The chunks are sent as soon as they are available.
                            The chunks are sent in the order they are received from the AI Service.
                            To improve the TTFB (Time-To-First-Byte), the chunk size starts from 1 and doubles until it reaches the max-chunks-per-message value.
                            """,
            defaultValue = "20")
    @JsonProperty(value = "min-chunks-per-message")
    private int minChunksPerMessage = 20;

    @ConfigProperty(
            description =
                    """
                            Field to use to store the completion results in the output topic. Use "value" to write the result without a structured schema. Use "value.<field>" to write the result in a specific field.
                            """)
    @JsonProperty(value = "completion-field")
    private String completionField;

    @ConfigProperty(
            description =
                    """
                            Enable streaming of the results. Use in conjunction with the stream-to-topic parameter.
                            """,
            defaultValue = "true")
    private boolean stream = true;

    @ConfigProperty(
            description =
                    """
                            Field to use to store the log of the completion results in the output topic. Use "value" to write the result without a structured schema. Use "value.<field>" to write the result in a specific field.
                            The log contains useful information for debugging the completion prompts.
                            """)
    @JsonProperty(value = "log-field")
    private String logField;

    @ConfigProperty(
            description =
                    """
                            Parameter for the completion request. The parameters are passed to the AI Service as is.
                            """)
    @JsonProperty(value = "max-tokens")
    private Integer maxTokens;

    @ConfigProperty(
            description =
                    """
                            Parameter for the completion request. The parameters are passed to the AI Service as is.
                            """)
    private Double temperature;

    @ConfigProperty(
            description =
                    """
                            Parameter for the completion request. The parameters are passed to the AI Service as is.
                            """)
    @JsonProperty(value = "top-p")
    private Double topP;

    @ConfigProperty(
            description =
                    """
                            Parameter for the completion request. The parameters are passed to the AI Service as is.
                            """)
    @JsonProperty(value = "logit-bias")
    private Map<String, Integer> logitBias;

    @ConfigProperty(
            description =
                    """
                            Parameter for the completion request. The parameters are passed to the AI Service as is.
                            """)
    @JsonProperty(value = "user")
    private String user;

    @ConfigProperty(
            description =
                    """
                            Parameter for the completion request. The parameters are passed to the AI Service as is.
                            """)
    @JsonProperty(value = "stop")
    private List<String> stop;

    @ConfigProperty(
            description =
                    """
                            Parameter for the completion request. The parameters are passed to the AI Service as is.
                            """)
    @JsonProperty(value = "presence-penalty")
    private Double presencePenalty;

    @ConfigProperty(
            description =
                    """
                            Parameter for the completion request. The parameters are passed to the AI Service as is.
                            """)
    @JsonProperty(value = "frequency-penalty")
    private Double frequencyPenalty;

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
                    Additional options for the model configuration. The structure depends on the model and AI provider.
                    """)
    @JsonProperty(value = "options")
    private Map<String, Object> options;
}
