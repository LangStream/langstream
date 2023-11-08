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
package ai.langstream.api.runner.code;

import java.util.List;
import java.util.Map;

/**
 * Base class for AgentCode implementations. It provides default implementations for the Agent
 * identity and AgentInfo methods.
 */
public abstract class AbstractAgentCode implements AgentCode {
    private MetricsReporter.Counter totalIn;
    private MetricsReporter.Counter totalOut;
    private String agentId;
    private String agentType;
    private long startedAt;
    private long lastProcessedAt;

    protected AgentContext agentContext;

    @Override
    public final String agentId() {
        return agentId;
    }

    @Override
    public final String agentType() {
        return agentType;
    }

    public final long startedAt() {
        return startedAt;
    }

    @Override
    public final void setMetadata(String id, String agentType, long startedAt) {
        this.agentId = id;
        this.agentType = agentType;
        this.startedAt = startedAt;
    }

    @Override
    public void setContext(AgentContext context) throws Exception {
        this.agentContext = context;

        totalIn = MetricsReporter.Counter.NOOP;
        totalOut = MetricsReporter.Counter.NOOP;

        // this is the main reported for the executor, we can use it to report metrics
        // about the whole execution in the pipeline
        MetricsReporter reporter = context.getMetricsReporter();
        switch (componentType()) {
            case SOURCE -> totalOut =
                    reporter.counter("source_out", "Total number of records emitted by the source");
            case SINK -> totalIn =
                    reporter.counter("sink_in", "Total number of records received by the sink");
        }
    }

    public void processed(long countIn, long countOut) {
        if (totalIn == null) {
            throw new IllegalStateException("setContext has not been called");
        }
        lastProcessedAt = System.currentTimeMillis();
        totalIn.count(countIn);
        totalOut.count(countOut);
    }

    /**
     * Override this method to provide additional information about the agent.
     *
     * @return a map of additional information
     */
    protected Map<String, Object> buildAdditionalInfo() {
        return Map.of();
    }

    @Override
    public List<AgentStatusResponse> getAgentStatus() {
        return List.of(
                new AgentStatusResponse(
                        agentId(),
                        agentType(),
                        componentType().name(),
                        buildAdditionalInfo(),
                        new AgentStatusResponse.Metrics(
                                totalIn.value(), totalOut.value(), startedAt(), lastProcessedAt)));
    }
}
