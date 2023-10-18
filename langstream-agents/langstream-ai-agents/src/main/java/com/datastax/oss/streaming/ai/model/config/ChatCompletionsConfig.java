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
package com.datastax.oss.streaming.ai.model.config;

import com.datastax.oss.streaming.ai.completions.ChatMessage;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;
import lombok.Data;

@Data
public class ChatCompletionsConfig extends StepConfig {

    @JsonProperty(required = true)
    private String model;

    @JsonProperty(value = "messages", required = true)
    private List<ChatMessage> messages;

    @JsonProperty(value = "stream-to-topic")
    private String streamToTopic;

    @JsonProperty(value = "stream-response-completion-field")
    private String streamResponseCompletionField;

    @JsonProperty(value = "min-chunks-per-message")
    private int minChunksPerMessage = 20;

    @JsonProperty(value = "completion-field")
    private String fieldName;

    @JsonProperty(value = "stream")
    private boolean stream = true;

    @JsonProperty(value = "log-field")
    private String logField;

    @JsonProperty(value = "max-tokens")
    private Integer maxTokens;

    @JsonProperty(value = "temperature")
    private Double temperature;

    @JsonProperty(value = "top-p")
    private Double topP;

    @JsonProperty(value = "logit-bias")
    private Map<String, Integer> logitBias;

    @JsonProperty(value = "user")
    private String user;

    @JsonProperty(value = "stop")
    private List<String> stop;

    @JsonProperty(value = "presence-penalty")
    private Double presencePenalty;

    @JsonProperty(value = "frequency-penalty")
    private Double frequencyPenalty;

    @JsonProperty(value = "options")
    private Map<String, Object> options;
}
