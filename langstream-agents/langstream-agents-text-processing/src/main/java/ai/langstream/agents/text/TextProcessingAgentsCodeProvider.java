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
package ai.langstream.agents.text;

import ai.langstream.api.runner.code.AgentCodeProvider;
import ai.langstream.api.runner.code.SingleRecordAgentProcessor;
import java.util.Map;
import java.util.function.Supplier;

public class TextProcessingAgentsCodeProvider implements AgentCodeProvider {

    private static final Map<String, Supplier<SingleRecordAgentProcessor>> FACTORIES =
            Map.of(
                    "text-extractor", TikaTextExtractorAgent::new,
                    "language-detector", LanguageDetectorAgent::new,
                    "text-splitter", TextSplitterAgent::new,
                    "text-normaliser", TextNormaliserAgent::new,
                    "document-to-json", DocumentToJsonAgent::new);

    @Override
    public boolean supports(String agentType) {
        return FACTORIES.containsKey(agentType);
    }

    @Override
    public SingleRecordAgentProcessor createInstance(String agentType) {
        return FACTORIES.get(agentType).get();
    }
}
