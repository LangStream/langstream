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
package com.datastax.oss.sga.api.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
public class ApplicationStatus {

    @Data
    public static class AgentStatus {
        private AgentLifecycleStatus status;
        private Map<String, AgentWorkerStatus> workers;

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AgentNodeStatus {
        @JsonProperty("agent-id")
        private String agentId;
        @JsonProperty("agent-type")
        private String agentType;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AgentWorkerStatus {

        private Status status;
        private String reason;
        // do not report this to users
        @JsonIgnore
        private String url;
        private Map<String, Object> info;

        private List<AgentNodeStatus> agents = new ArrayList<>();

        public static final AgentWorkerStatus INITIALIZING() {
            return new AgentWorkerStatus(Status.INITIALIZING, null,null,Map.of(), List.of());
        }

        public static final AgentWorkerStatus RUNNING(String url) {
            return new AgentWorkerStatus(Status.RUNNING, null, url, Map.of(), List.of());
        }

        public static final AgentWorkerStatus ERROR(String url, String reason) {
            return new AgentWorkerStatus(Status.ERROR, reason, url, Map.of(), List.of());
        }

        public AgentWorkerStatus withInfo(Map<String, Object> info) {
            return new AgentWorkerStatus(this.status, this.reason, this.url, info, this.agents);
        }

        public AgentWorkerStatus withAgentSpec(String agentId, String agentType, Map<String, Object> configuration) {
            List<AgentNodeStatus> newAgents = new ArrayList<>();
            if ("composite-agent".equals(agentType)) {
                // this is a little hacky, the composite agent is a special case
                if (configuration != null) {
                    Map<String, Object> source = (Map<String, Object>) configuration.get("source");
                    if (source != null && !source.isEmpty()) {
                        newAgents.add(new AgentNodeStatus((String) source.get("agentId"), (String) source.get("agentType")));
                    }
                    List<Map<String, Object>> processors = (List<Map<String, Object>>) configuration.get("processors");
                    if (processors != null) {
                        for (Map<String, Object> agent : processors) {
                            newAgents.add(new AgentNodeStatus((String) agent.get("agentId"), (String) agent.get("agentType")));
                        }
                    }
                    Map<String, Object> sink = (Map<String, Object>) configuration.get("sink");
                    if (sink != null && !sink.isEmpty()) {
                        newAgents.add(new AgentNodeStatus((String) sink.get("agentId"), (String) sink.get("agentType")));
                    }
                }
            } else {
                newAgents.add(new AgentNodeStatus(agentId, agentType));
            }
            return new AgentWorkerStatus(this.status, this.reason, this.url, this.info, newAgents);
        }

        public enum Status {
            INITIALIZING,
            RUNNING,
            ERROR;
        }

    }

    private ApplicationLifecycleStatus status;
    private Map<String, AgentStatus> agents;
}
