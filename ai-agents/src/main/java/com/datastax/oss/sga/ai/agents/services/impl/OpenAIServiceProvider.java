package com.datastax.oss.sga.ai.agents.services.impl;

import com.azure.ai.openai.OpenAIClient;
import com.datastax.oss.sga.ai.agents.services.ServiceProviderProvider;
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
    public ServiceProvider createImplementation(Map<String, Object> agentConfiguration) {
        OpenAIConfig config = TransformFunctionUtil
                .convertFromMap((Map<String, Object>) agentConfiguration.get("openai"), OpenAIConfig.class);
        OpenAIClient client = TransformFunctionUtil.buildOpenAIClient(config);
        return new com.datastax.oss.streaming.ai.services.OpenAIServiceProvider(client);
    }
}
