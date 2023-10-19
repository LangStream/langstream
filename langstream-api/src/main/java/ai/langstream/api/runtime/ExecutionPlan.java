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
package ai.langstream.api.runtime;

import ai.langstream.api.model.Application;
import ai.langstream.api.model.Connection;
import ai.langstream.api.model.Module;
import ai.langstream.api.model.TopicDefinition;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/** This is the implementation of an application that can be deployed on a RuntimeCluster */
@Slf4j
@Data
public final class ExecutionPlan {

    private final Map<TopicDefinition, Topic> topics = new HashMap<>();
    private final List<AssetNode> assets = new ArrayList<>();
    private final Map<String, AgentNode> agents = new HashMap<>();
    private final String applicationId;
    private final Application application;

    public ExecutionPlan(String applicationId, Application applicationInstance) {
        this.applicationId = applicationId;
        this.application = applicationInstance;
    }

    /**
     * Get reference to the source application instance
     *
     * @return the source application definition
     */
    public Application getApplication() {
        return application;
    }

    /**
     * Get a connection implementation.
     *
     * @param module the module
     * @param connection the connection
     * @return the connection implementation
     */
    public ConnectionImplementation getConnectionImplementation(
            Module module, Connection connection) {
        return switch (connection.connectionType()) {
            case AGENT -> getAgentImplementation(module, connection.definition());
            case TOPIC -> getTopicByName(connection.definition());
        };
    }

    /**
     * Get all the Logical Topics to be deployed on the StreamingCluster
     *
     * @return the topics to be deployed on the StreamingCluster
     */
    public List<Topic> getLogicalTopics() {
        return new ArrayList<>(topics.values());
    }

    /**
     * Register the implementation of a topic
     *
     * @param topicDefinition the topic definition
     * @param topicImplementation the topic implementation
     */
    public void registerTopic(TopicDefinition topicDefinition, Topic topicImplementation) {
        topics.put(topicDefinition, topicImplementation);
    }

    /**
     * Discard a topic implementation
     *
     * @param topicImplementation the topic implementation
     */
    public void discardTopic(ConnectionImplementation topicImplementation) {
        topics.entrySet().stream()
                .filter(
                        e -> {
                            boolean res = e.getValue().equals(topicImplementation);
                            if (log.isDebugEnabled()) {
                                log.debug(
                                        "Compare {} with {}: {}",
                                        topicImplementation,
                                        e.getValue(),
                                        res);
                            }
                            return res;
                        })
                .findFirst()
                .ifPresent(e -> topics.remove(e.getKey()));
    }

    /**
     * Get an existing agent implementation
     *
     * @param module the module
     * @param id the id of the agent
     * @return the agent
     */
    public AgentNode getAgentImplementation(Module module, String id) {
        return agents.get(module.getId() + "#" + id);
    }

    public void registerAgent(Module module, String id, AgentNode agentImplementation) {
        String internalId = module.getId() + "#" + id;
        if (log.isDebugEnabled()) {
            log.debug(
                    "registering agent {} for module {} with id {}",
                    agentImplementation,
                    module.getId(),
                    id);
        }
        agents.put(internalId, agentImplementation);
    }

    public Map<String, AgentNode> getAgents() {
        return agents;
    }

    public Topic getTopicByName(String name) {
        return topics.entrySet().stream()
                .filter(e -> e.getKey().getName().equals(name))
                .findFirst()
                .map(Map.Entry::getValue)
                .orElse(null);
    }

    public TopicDefinition getTopicDefinitionByName(String name) {
        return topics.entrySet().stream()
                .filter(e -> e.getValue().topicName().equals(name))
                .findFirst()
                .map(Map.Entry::getKey)
                .orElse(null);
    }

    public List<AssetNode> getAssets() {
        return assets;
    }

    public void registerAsset(AssetNode asset) {
        assets.add(asset);
    }
}
