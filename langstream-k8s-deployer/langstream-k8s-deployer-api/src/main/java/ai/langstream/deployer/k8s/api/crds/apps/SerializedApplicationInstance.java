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
package ai.langstream.deployer.k8s.api.crds.apps;

import ai.langstream.api.model.Application;
import ai.langstream.api.model.Gateways;
import ai.langstream.api.model.Instance;
import ai.langstream.api.model.Module;
import ai.langstream.api.model.Resource;
import ai.langstream.api.model.ResourcesSpec;
import ai.langstream.api.runtime.AgentNode;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.Topic;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Data
@NoArgsConstructor
@Slf4j
public class SerializedApplicationInstance {

    public SerializedApplicationInstance(
            Application applicationInstance, ExecutionPlan executionPlan) {
        this.resources = applicationInstance.getResources();
        this.modules = applicationInstance.getModules();
        this.instance = applicationInstance.getInstance();
        this.gateways = applicationInstance.getGateways();
        this.agentRunners = new HashMap<>();
        if (log.isDebugEnabled()) {
            log.debug(
                    "Serializing application instance {} executionPlan {}",
                    applicationInstance,
                    executionPlan);
        }
        if (executionPlan != null && executionPlan.getAgents() != null) {
            for (Map.Entry<String, AgentNode> entry : executionPlan.getAgents().entrySet()) {
                AgentNode agentNode = entry.getValue();
                AgentRunnerDefinition agentRunnerDefinition = new AgentRunnerDefinition();
                agentRunnerDefinition.setAgentId(agentNode.getId());
                agentRunnerDefinition.setAgentType(agentNode.getAgentType());
                agentRunnerDefinition.setComponentType(agentNode.getComponentType() + "");
                agentRunnerDefinition.setConfiguration(
                        agentNode.getConfiguration() != null
                                ? agentNode.getConfiguration()
                                : Map.of());
                agentRunnerDefinition.setResources(agentNode.getResources());

                agentRunners.put(entry.getKey(), agentRunnerDefinition);

                if (agentNode.getInputConnectionImplementation() instanceof Topic topic) {
                    agentRunnerDefinition.setInputTopic(topic.topicName());
                }

                if (agentNode.getOutputConnectionImplementation() instanceof Topic topic) {
                    agentRunnerDefinition.setOutputTopic(topic.topicName());
                }
            }
        }
    }

    private Map<String, Resource> resources = new HashMap<>();
    private Map<String, Module> modules = new HashMap<>();
    private Instance instance;
    private Gateways gateways;
    private Map<String, AgentRunnerDefinition> agentRunners;

    public Application toApplicationInstance() {
        final Application app = new Application();
        app.setInstance(instance);
        app.setModules(modules);
        app.setResources(resources);
        app.setGateways(gateways);
        return app;
    }

    public Map<String, AgentRunnerDefinition> getAgentRunners() {
        return agentRunners;
    }

    @Data
    public static class AgentRunnerDefinition {
        private String agentId;
        private String agentType;
        private String componentType;
        private Map<String, Object> configuration;
        private String inputTopic;
        private String outputTopic;
        private ResourcesSpec resources;
    }
}
