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
package ai.langstream.kafka;

import ai.langstream.api.runner.code.AgentCode;
import ai.langstream.api.runner.code.AgentCodeProvider;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SingleRecordAgentProcessor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Objects;

@Slf4j
public class FailingProcessorAgentCodeProvider implements AgentCodeProvider {
    @Override
    public boolean supports(String agentType) {
        return "mock-failing-processor".equals(agentType);
    }

    @Override
    public AgentCode createInstance(String agentType) {
        return new SingleRecordAgentProcessor() {

            String failOnContent;
            @Override
            public void init(Map<String, Object> configuration) {
                failOnContent = configuration.getOrDefault("fail-on-content", "").toString();
            }

            @Override
            public List<Record> processRecord(Record record) {
                log.info("Processing record value {}, failOnContent {}", record.value(), failOnContent);
                if (Objects.equals(record.value(), failOnContent)) {
                    throw new RuntimeException("Failing on content: " + failOnContent);
                }
                if (record.value() instanceof String s) {
                    if (s.contains(failOnContent)) {
                        throw new RuntimeException("Failing on content: " + failOnContent);
                    }
                }
                return List.of(record);
            }
        };
    }
}
