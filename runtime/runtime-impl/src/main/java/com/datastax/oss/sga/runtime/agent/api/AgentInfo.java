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
package com.datastax.oss.sga.runtime.agent.api;

import com.datastax.oss.sga.api.runner.code.AgentCode;
import com.datastax.oss.sga.api.runner.code.AgentProcessor;
import com.datastax.oss.sga.api.runner.code.AgentSink;
import com.datastax.oss.sga.api.runner.code.AgentSource;
import com.datastax.oss.sga.api.runner.topics.TopicConsumer;
import com.datastax.oss.sga.api.runner.topics.TopicProducer;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class AgentInfo {
    private AgentProcessor processor;
    private AgentSource source;
    private AgentSink sink;


    public void watchProcessor(AgentProcessor processor) {
        this.processor = processor;
    }

    public void watchSource(AgentSource source) {
        this.source = source;
    }

    public void watchSink(AgentSink sink) {
        this.sink = sink;
    }


    /**
     * This is serving the data to the Control Plane,
     * changing the format is a breaking change, please take care to backward compatibility.
     * @return
     */
    public Map<String, Object> serveInfos() {
        Map<String, Object> result = new LinkedHashMap<>();
        if (source != null) {
            result.put("source", source.getInfo());
        }
        if (processor != null) {
            result.put("processor", processor.getInfo());
        }
        if (sink != null) {
            result.put("sink", sink.getInfo());
        }
        return result;
    }

}
