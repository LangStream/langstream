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
package ai.langstream.runtime.agent.simple;

import ai.langstream.api.runner.code.AgentCode;
import ai.langstream.api.runner.code.AgentCodeProvider;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SingleRecordAgentProcessor;
import java.util.List;

public class IdentityAgentProvider implements AgentCodeProvider {
    @Override
    public boolean supports(String agentType) {
        return "identity".equals(agentType);
    }

    @Override
    public AgentCode createInstance(String agentType) {
        return new IdentityAgentCode();
    }

    public static class IdentityAgentCode extends SingleRecordAgentProcessor {

        @Override
        public List<Record> processRecord(Record record) {
            return List.of(record);
        }
    }
}
