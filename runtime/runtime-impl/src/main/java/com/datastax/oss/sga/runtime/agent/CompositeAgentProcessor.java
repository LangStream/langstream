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
package com.datastax.oss.sga.runtime.agent;

import com.datastax.oss.sga.api.runner.code.AgentContext;
import com.datastax.oss.sga.api.runner.code.AgentProcessor;
import com.datastax.oss.sga.api.runner.code.AgentSink;
import com.datastax.oss.sga.api.runner.code.AgentSource;
import com.datastax.oss.sga.api.runner.code.Record;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This is a special processor that executes a pipeline of Agents in memory.
 */
public class CompositeAgentProcessor implements AgentProcessor {

    private AgentSource source;
    private final List<AgentProcessor> processors = new ArrayList<>();
    private AgentSink sink;
    @Override
    public void init(Map<String, Object> configuration) throws Exception {
        List<Map<String, Object>> processorsDefinition = null;
        if (configuration.containsKey("processors")) {
            processorsDefinition = (List<Map<String, Object>>) configuration.get("processors");
        }
        if (processorsDefinition == null) {
            processorsDefinition = List.of();
        }
        Map<String, Object> sourceDefinition = (Map<String, Object>) configuration.get("source");
        if (sourceDefinition == null) {
            sourceDefinition = Map.of();
        }
        Map<String, Object> sinkDefinition = (Map<String, Object>) configuration.get("sink");
        if (sinkDefinition == null) {
            sinkDefinition = Map.of();
        }

        if (!sourceDefinition.isEmpty()) {
            String agentType = (String) sourceDefinition.get("agentType");
            Map<String, Object> agentConfiguration = (Map<String, Object>) sourceDefinition.get("configuration");
            source = (AgentSource) AgentRunner.initAgent(agentType, agentConfiguration);
        }

        for (Map<String, Object> agentDefinition : processorsDefinition) {
            String agentType = (String) agentDefinition.get("agentType");
            Map<String, Object> agentConfiguration = (Map<String, Object>) agentDefinition.get("configuration");
            AgentProcessor agent = (AgentProcessor) AgentRunner.initAgent(agentType, agentConfiguration);
            processors.add(agent);
        }

        if (!sinkDefinition.isEmpty()) {
            String agentType = (String) sinkDefinition.get("agentType");
            Map<String, Object> agentConfiguration = (Map<String, Object>) sinkDefinition.get("configuration");
            sink = (AgentSink) AgentRunner.initAgent(agentType, agentConfiguration);
        }
    }

    public AgentSource getSource() {
        return source;
    }

    public List<AgentProcessor> getProcessors() {
        return processors;
    }

    public AgentSink getSink() {
        return sink;
    }

    @Override
    public void setContext(AgentContext context) throws Exception {
        // we are not setting the Context on wrapped Agents
        // the context would allow them to access the Consumers and the Producers
        // and this functionality is not supported in this processor
    }

    @Override
    public void start() throws Exception {
        for (AgentProcessor agent : processors) {
            agent.start();
        }
    }

    @Override
    public void close() throws Exception {
        for (AgentProcessor agent : processors) {
            agent.close();
        }
    }

    @Override
    public List<SourceRecordAndResult> process(List<Record> records) {
        if (processors.isEmpty()) {
            return records
                    .stream()
                    .map(r -> new SourceRecordAndResult(r, List.of(r), null))
                    .toList();
        }

        int current = 0;
        AgentProcessor currentAgent = processors.get(current++);
        List<SourceRecordAndResult> currentResults;
        try {
            currentResults = currentAgent.process(records);
        } catch (Throwable error) {
            // first stage errored out
            return records
                    .stream()
                    .map(r -> new SourceRecordAndResult(r, null, error))
                    .toList();
        }

        // we must preserve the original mapping to the Source Record
        // each SourceRecord generates some SinkRecords
        while (current < processors.size()) {
            List<SourceRecordAndResult>  nextStageResults = new ArrayList<>();

            currentAgent = processors.get(current++);
            for (SourceRecordAndResult entry : currentResults) {
                // we pass to the next agent only records generated by the same SourceRecord
                // this reduces a lot the complexity of the code, but it defeats micro-batching
                // this algorithm can be improved in the future
                Record sourceRecord = entry.getSourceRecord();
                List<Record> sinkRecords = entry.getResultRecords();
                if (entry.getError() != null) {
                    nextStageResults.add(new SourceRecordAndResult(sourceRecord,
                            null, entry.getError()));
                } else {
                    try {
                        List<SourceRecordAndResult> processed = currentAgent.process(sinkRecords);
                        nextStageResults.add(new SourceRecordAndResult(sourceRecord,
                                processed
                                        .stream()
                                        .map(SourceRecordAndResult::getResultRecords)
                                        .flatMap(List::stream).toList(), null));
                    } catch (Throwable error) {
                        nextStageResults.add(new SourceRecordAndResult(sourceRecord,
                                null, error));
                    }
                }

            }
            currentResults = nextStageResults;
        }

        return currentResults;
    }

}
