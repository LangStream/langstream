package com.dastastax.oss.sga.agents.tika;

import com.datastax.oss.sga.api.runner.code.AgentCodeProvider;
import com.datastax.oss.sga.api.runner.code.SingleRecordAgentFunction;

import java.util.Map;
import java.util.function.Supplier;

public class TikaAgentsCodeProvider implements AgentCodeProvider {

    private static Map<String, Supplier<SingleRecordAgentFunction>> FACTORIES = Map.of(
            "text-extractor", () -> new TikaTextExtractorAgent(),
            "language-detector", () -> new LanguageDetectorAgent(),
            "text-chunker", () -> new TextChunkerAgent()
    );

    @Override
    public boolean supports(String agentType) {
        return FACTORIES.containsKey(agentType);
    }

    @Override
    public SingleRecordAgentFunction createInstance(String agentType) {
        return FACTORIES.get(agentType).get();
    }
}
