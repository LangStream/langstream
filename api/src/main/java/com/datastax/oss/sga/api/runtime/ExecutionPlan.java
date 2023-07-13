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
package com.datastax.oss.sga.api.runtime;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.TopicDefinition;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is the implementation of an application that can be deployed on a RuntimeCluster
 */
@Slf4j
@Data
public final class ExecutionPlan {

    private final Map<TopicDefinition, Topic> topics = new HashMap<>();
    private final Map<String, AgentNode> agents = new HashMap<>();
    protected final Application application;

    public ExecutionPlan(Application applicationInstance) {
        this.application = applicationInstance;
    }

    /**
     * Get reference to the source application instance
     * @return the source application definition
     */
    public Application getApplication() {
        return application;
    }

    /**
     * Get a connection
     * @param module
     * @param connection
     * @return the connection implementation
     */
    public Connection getConnectionImplementation(Module module, com.datastax.oss.sga.api.model.Connection connection) {
        com.datastax.oss.sga.api.model.Connection.Connectable endpoint = connection.endpoint();
        if (endpoint.getConnectableType() == null) {
            throw new IllegalArgumentException("Connection " + connection + " has no connectable endpoint");
        }
        switch (endpoint.getConnectableType()) {
            case com.datastax.oss.sga.api.model.Connection.Connectables.AGENT:
                return getAgentImplementation(module, ((AgentConfiguration) endpoint).getId());
            case com.datastax.oss.sga.api.model.Connection.Connectables.TOPIC:
                return getTopicByName(((TopicDefinition) endpoint).getName());
            default:
                throw new IllegalArgumentException("Unknown connectable type " + endpoint.getConnectableType());
        }
    }

    /**
     * Get all the Logical Topics to be deployed on the StreamingCluster
     * @return the topics to be deployed on the StreamingCluster
     */
    public List<Topic> getLogicalTopics() {
        return new ArrayList<>(topics.values());
    }

    /**
     * Register the implementation of a topic
     * @param topicDefinition
     * @param topicImplementation
     */
    public void registerTopic(TopicDefinition topicDefinition, Topic topicImplementation) {
        topics.put(topicDefinition, topicImplementation);
    }

    /**
     * Discard a topic implementation
     * @param topicImplementation
     */
    public void discardTopic(Connection topicImplementation) {
        topics.entrySet()
                .stream()
                .filter(e -> e.getValue().equals(topicImplementation)).findFirst()
                .ifPresent(e -> topics.remove(e.getKey()));
    }

    /**
     * Get an existing agent implementation
     * @param module
     * @param id
     * @return the agent
     */
    public AgentNode getAgentImplementation(Module module, String id) {
        return agents.get(module.getId() + "#" + id);
    }

    public void registerAgent(Module module, String id, AgentNode agentImplementation) {
        String internalId = module.getId() + "#" + id;
        log.info("registering agent {} for module {} with id {}", agentImplementation, module.getId(), id);
        agents.put(internalId, agentImplementation);
    }

    public Map<String, AgentNode> getAgents() {
        return agents;
    }

    public Topic getTopicByName(String name) {
        return topics
                .entrySet()
                .stream()
                .filter(e -> e.getKey().getName().equals(name))
                .findFirst()
                .map(Map.Entry::getValue)
                .orElse(null);
    }
}
