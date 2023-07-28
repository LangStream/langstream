package com.dastastax.oss.sga.agents.tika;

import com.datastax.oss.sga.api.runner.code.AgentCodeProvider;
import com.datastax.oss.sga.api.runner.code.SingleRecordAgentProcessor;

import java.util.Map;
import java.util.function.Supplier;

public class TextProcessingAgentsCodeProvider implements AgentCodeProvider {

    private static Map<String, Supplier<SingleRecordAgentProcessor>> FACTORIES = Map.of(
            "text-extractor", () -> new TikaTextExtractorAgent(),
            "language-detector", () -> new LanguageDetectorAgent(),
            "text-splitter", () -> new TextSplitterAgent(),
            "text-normaliser", () -> new TextNormaliserAgent(),
            "document-to-json", () -> new DocumentToJsonAgent()
    );

    @Override
    public boolean supports(String agentType) {
        return FACTORIES.containsKey(agentType);
    }

    @Override
    public SingleRecordAgentProcessor createInstance(String agentType) {
        return FACTORIES.get(agentType).get();
    }
}
