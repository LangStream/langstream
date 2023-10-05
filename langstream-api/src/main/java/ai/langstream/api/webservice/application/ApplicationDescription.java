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
package ai.langstream.api.webservice.application;

import ai.langstream.api.model.AgentLifecycleStatus;
import ai.langstream.api.model.Application;
import ai.langstream.api.model.ApplicationLifecycleStatus;
import ai.langstream.api.model.ApplicationStatus;
import ai.langstream.api.model.Gateways;
import ai.langstream.api.model.Instance;
import ai.langstream.api.model.Module;
import ai.langstream.api.model.Pipeline;
import ai.langstream.api.model.Resource;
import ai.langstream.api.model.TopicDefinition;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ApplicationDescription {

    @JsonProperty("application-id")
    private String applicationId;

    @JsonProperty("application")
    private ApplicationDefinition application;

    @JsonProperty("status")
    private AgentStatusDescription status;

    public ApplicationDescription(
            String applicationId, Application application, ApplicationStatus status) {
        this.applicationId = applicationId;
        this.application = new ApplicationDefinition(application);
        this.status = new AgentStatusDescription(status);
    }

    @Data
    @NoArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ApplicationDefinition {

        public ApplicationDefinition(Application application) {
            this.resources = application.getResources();
            this.modules =
                    application.getModules().values().stream().map(ModuleDefinition::new).toList();
            this.gateways = application.getGateways();
            this.instance = application.getInstance();
        }

        private Map<String, Resource> resources;
        private List<ModuleDefinition> modules;
        private Gateways gateways;
        private Instance instance;
    }

    @Data
    @NoArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ModuleDefinition {
        private String id;
        private List<Pipeline> pipelines;

        private List<TopicDefinition> topics;

        ModuleDefinition(Module module) {
            this.id = module.getId();
            this.pipelines = new ArrayList<>(module.getPipelines().values());
            this.topics = new ArrayList<>(module.getTopics().values());
        }
    }

    @Data
    @NoArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class AgentStatusDescription {
        private ApplicationLifecycleStatus status;
        private List<ExecutorDescription> executors;

        public AgentStatusDescription(ApplicationStatus status) {
            this.status = status.getStatus();
            this.executors =
                    status.getAgents().entrySet().stream()
                            .map(entry -> new ExecutorDescription(entry.getKey(), entry.getValue()))
                            .sorted(Comparator.comparing(ExecutorDescription::getId))
                            .toList();
        }
    }

    @Data
    @NoArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ExecutorDescription {
        private String id;
        private AgentLifecycleStatus status;
        private List<ReplicaStatus> replicas;

        public ExecutorDescription(String id, ApplicationStatus.AgentStatus status) {
            this.id = id;
            this.status = status.getStatus();
            this.replicas =
                    status.getWorkers().entrySet().stream()
                            .map(entry -> new ReplicaStatus(entry.getKey(), entry.getValue()))
                            .sorted(Comparator.comparing(ReplicaStatus::getId))
                            .toList();
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ReplicaStatus {

        private String id;
        private ApplicationStatus.AgentWorkerStatus.Status status;
        private String reason;
        // do not report this to users
        @JsonIgnore private String url;

        private List<AgentStatus> agents = new ArrayList<>();

        public ReplicaStatus(String id, ApplicationStatus.AgentWorkerStatus workerStatus) {
            this.id = id;
            this.status = workerStatus.getStatus();
            this.reason = workerStatus.getReason();
            this.url = workerStatus.getUrl();
            this.agents =
                    workerStatus.getAgents().stream()
                            .map(AgentStatus::new)
                            .collect(Collectors.toList());
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class AgentStatus {
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

        public AgentStatus(ApplicationStatus.AgentNodeStatus agentNodeStatus) {
            this.agentId = agentNodeStatus.getAgentId();
            this.agentType = agentNodeStatus.getAgentType();
            this.componentType = agentNodeStatus.getComponentType();
            this.metrics =
                    agentNodeStatus.getMetrics() != null
                            ? new Metrics(agentNodeStatus.getMetrics())
                            : null;
            this.info = agentNodeStatus.getInfo();
        }
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

        public Metrics(ApplicationStatus.AgentNodeStatus.Metrics metrics) {
            if (metrics != null) {
                this.totalIn = metrics.getTotalIn();
                this.totalOut = metrics.getTotalOut();
                this.startedAt = metrics.getStartedAt();
                this.lastProcessedAt = metrics.getLastProcessedAt();
            }
        }
    }
}
