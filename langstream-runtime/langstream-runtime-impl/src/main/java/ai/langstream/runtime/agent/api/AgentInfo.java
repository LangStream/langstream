/**
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
package ai.langstream.runtime.agent.api;

import ai.langstream.api.runner.code.AgentCode;
import ai.langstream.api.runner.code.AgentStatusResponse;
import ai.langstream.api.runner.code.AgentProcessor;
import ai.langstream.api.runner.code.AgentSink;
import ai.langstream.api.runner.code.AgentSource;

import java.util.ArrayList;
import java.util.List;

public class AgentInfo {
    private AgentProcessor processor;
    private AgentCode source;
    private AgentCode sink;


    public void watchProcessor(AgentProcessor processor) {
        this.processor = processor;
    }

    public void watchSource(AgentCode source) {
        this.source = source;
    }

    public void watchSink(AgentCode sink) {
        this.sink = sink;
    }

    /**
     * This is serving the data to the Control Plane,
     * changing the format is a breaking change, please take care to backward compatibility.
     * @return
     */
    public List<AgentStatusResponse> serveWorkerStatus() {
        List<AgentStatusResponse> result = new ArrayList<>();
        if (source != null) {
            result.addAll(source.getAgentStatus());
        }
        if (processor != null) {
            result.addAll(processor.getAgentStatus());
        }
        if (sink != null) {
            result.addAll(sink.getAgentStatus());
        }
        return result;
    }

}
