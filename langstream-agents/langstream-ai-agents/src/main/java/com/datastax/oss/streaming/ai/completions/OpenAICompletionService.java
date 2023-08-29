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
package com.datastax.oss.streaming.ai.completions;

import static com.datastax.oss.streaming.ai.util.TransformFunctionUtil.getDouble;
import static com.datastax.oss.streaming.ai.util.TransformFunctionUtil.getInteger;

import com.azure.ai.openai.OpenAIClient;
import com.azure.ai.openai.models.ChatCompletionsOptions;
import com.azure.ai.openai.models.ChatRole;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OpenAICompletionService implements CompletionsService {

    private OpenAIClient client;

    public OpenAICompletionService(OpenAIClient client) {
        this.client = client;
    }

    @Override
    public ChatCompletions getChatCompletions(
            List<ChatMessage> messages, Map<String, Object> options) {
        ChatCompletionsOptions chatCompletionsOptions =
                new ChatCompletionsOptions(
                                messages.stream()
                                        .map(
                                                message ->
                                                        new com.azure.ai.openai.models.ChatMessage(
                                                                        ChatRole.fromString(
                                                                                message.getRole()))
                                                                .setContent(message.getContent()))
                                        .collect(Collectors.toList()))
                        .setMaxTokens(getInteger("max-tokens", options))
                        .setTemperature(getDouble("temperature", options))
                        .setTopP(getDouble("top-p", options))
                        .setLogitBias((Map<String, Integer>) options.get("logit-bias"))
                        .setUser((String) options.get("user"))
                        .setStop((List<String>) options.get("stop"))
                        .setPresencePenalty(getDouble("presence-penalty", options))
                        .setFrequencyPenalty(getDouble("frequency-penalty", options));
        com.azure.ai.openai.models.ChatCompletions chatCompletions =
                client.getChatCompletions((String) options.get("model"), chatCompletionsOptions);
        ChatCompletions result = new ChatCompletions();
        result.setChoices(
                chatCompletions.getChoices().stream()
                        .map(c -> new ChatChoice(convertMessage(c)))
                        .collect(Collectors.toList()));
        return result;
    }

    private static ChatMessage convertMessage(com.azure.ai.openai.models.ChatChoice c) {
        com.azure.ai.openai.models.ChatMessage message = c.getMessage();
        return new ChatMessage(
                message.getRole() != null ? message.getRole().toString() : null,
                message.getContent());
    }
}
