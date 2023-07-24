package com.datastax.oss.sga.ai.agents.services.impl;

import com.azure.ai.openai.OpenAIClient;
import com.datastax.oss.sga.ai.agents.services.ServiceProviderProvider;
import com.datastax.oss.streaming.ai.model.config.OpenAIConfig;
import com.datastax.oss.streaming.ai.services.HuggingFaceServiceProvider;
import com.datastax.oss.streaming.ai.services.ServiceProvider;
import com.datastax.oss.streaming.ai.util.TransformFunctionUtil;

import java.util.Map;

public class HuggingFaceProvider implements ServiceProviderProvider {
    @Override
    public boolean supports(Map<String, Object> agentConfiguration) {
        return agentConfiguration.containsKey("huggingface");
    }

    @Override
    public ServiceProvider createImplementation(Map<String, Object> agentConfiguration) {
        return new HuggingFaceServiceProvider(agentConfiguration);
    }
}
