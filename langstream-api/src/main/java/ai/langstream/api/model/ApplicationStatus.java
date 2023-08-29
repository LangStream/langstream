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
package ai.langstream.api.model;

import ai.langstream.api.runner.code.AgentStatusResponse;
import ai.langstream.api.runtime.ComponentType;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
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

        @JsonProperty("component-type")
        private String componentType;

        @JsonProperty("metrics")
        private Metrics metrics;

        @JsonProperty("info")
        private Map<String, Object> info;

        public AgentNodeStatus(String agentId, String agentType, String componentType) {
            this.agentId = agentId;
            this.agentType = agentType;
            this.componentType = componentType;
        }

        public AgentNodeStatus(
                String agentId, String agentType, String componentType, Map<String, Object> info) {
            this.agentId = agentId;
            this.agentType = agentType;
            this.componentType = componentType;
            this.info = info;
        }

        @Data
        @AllArgsConstructor
        @NoArgsConstructor
        public static final class Metrics {
            @JsonProperty("total-in")
            private Long totalIn;

            @JsonProperty("total-out")
            private Long totalOut;

            @JsonProperty("started-at")
            private Long startedAt;

            @JsonProperty("last-processed-at")
            private Long lastProcessedAt;
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AgentWorkerStatus {

        private Status status;
        private String reason;
        // do not report this to users
        @JsonIgnore private String url;

        private List<AgentNodeStatus> agents = new ArrayList<>();

        public static final AgentWorkerStatus INITIALIZING() {
            return new AgentWorkerStatus(Status.INITIALIZING, null, null, List.of());
        }

        public static final AgentWorkerStatus RUNNING(String url) {
            return new AgentWorkerStatus(Status.RUNNING, null, url, List.of());
        }

        public static final AgentWorkerStatus ERROR(String url, String reason) {
            return new AgentWorkerStatus(Status.ERROR, reason, url, List.of());
        }

        public AgentWorkerStatus applyAgentStatus(List<AgentStatusResponse> agentsStatus) {
            List<AgentNodeStatus> newAgents = new ArrayList<>();
            for (AgentNodeStatus agentStatus : agents) {
                AgentStatusResponse responseForAgent =
                        agentsStatus.stream()
                                .filter(a -> a.getAgentId().equals(agentStatus.getAgentId()))
                                .findFirst()
                                .orElse(null);
                if (responseForAgent != null) {
                    if (!Objects.equals(
                            responseForAgent.getAgentType(), agentStatus.getAgentType())) {
                        log.warn(
                                "The response from the pod response that agent {} is of type {} instead of type {}",
                                responseForAgent.getAgentId(),
                                responseForAgent.getAgentType(),
                                agentStatus.getAgentType());
                    }
                    Map<String, Object> mergedInfo = new HashMap<>();
                    if (agentStatus.getInfo() != null) {
                        mergedInfo.putAll(agentStatus.getInfo());
                    }
                    if (responseForAgent.getInfo() != null) {
                        mergedInfo.putAll(responseForAgent.getInfo());
                    }
                    newAgents.add(
                            new AgentNodeStatus(
                                    agentStatus.getAgentId(),
                                    agentStatus.getAgentType(),
                                    agentStatus.getComponentType(),
                                    new AgentNodeStatus.Metrics(
                                            responseForAgent.getMetrics().getTotalIn(),
                                            responseForAgent.getMetrics().getTotalOut(),
                                            responseForAgent.getMetrics().getStartedAt(),
                                            responseForAgent.getMetrics().getLastProcessedAt()),
                                    mergedInfo));
                } else {
                    newAgents.add(agentStatus);
                }
            }
            return new AgentWorkerStatus(this.status, this.reason, this.url, newAgents);
        }

        public AgentWorkerStatus withAgentSpec(
                String agentId,
                String agentType,
                String componentType,
                Map<String, Object> configuration,
                String inputTopic,
                String outputTopic) {
            List<AgentNodeStatus> newAgents = new ArrayList<>();
            if (inputTopic != null) {
                newAgents.add(
                        new AgentNodeStatus(
                                "topic-source",
                                "topic-source",
                                ComponentType.SOURCE.name(),
                                Map.of("topic", inputTopic)));
            }
            if ("composite-agent".equals(agentType)) {
                // this is a little hacky, the composite agent is a special case
                if (configuration != null) {
                    Map<String, Object> source = (Map<String, Object>) configuration.get("source");
                    if (source != null && !source.isEmpty()) {
                        newAgents.add(
                                new AgentNodeStatus(
                                        (String) source.get("agentId"),
                                        (String) source.get("agentType"),
                                        (String) source.get("componentType")));
                    }
                    List<Map<String, Object>> processors =
                            (List<Map<String, Object>>) configuration.get("processors");
                    if (processors != null) {
                        for (Map<String, Object> agent : processors) {
                            newAgents.add(
                                    new AgentNodeStatus(
                                            (String) agent.get("agentId"),
                                            (String) agent.get("agentType"),
                                            (String) agent.get("componentType")));
                        }
                    }
                    Map<String, Object> sink = (Map<String, Object>) configuration.get("sink");
                    if (sink != null && !sink.isEmpty()) {
                        newAgents.add(
                                new AgentNodeStatus(
                                        (String) sink.get("agentId"),
                                        (String) sink.get("agentType"),
                                        (String) sink.get("componentType")));
                    }
                }
            } else {
                newAgents.add(new AgentNodeStatus(agentId, agentType, componentType));
            }

            if (outputTopic != null) {
                newAgents.add(
                        new AgentNodeStatus(
                                "topic-sink",
                                "topic-sink",
                                ComponentType.SINK.name(),
                                Map.of("topic", outputTopic)));
            }

            return new AgentWorkerStatus(this.status, this.reason, this.url, newAgents);
        }

        public enum Status {
            INITIALIZING,
            RUNNING,
            ERROR
        }
    }

    private ApplicationLifecycleStatus status;
    private Map<String, AgentStatus> agents;
}
