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
import com.datastax.oss.sga.api.model.ApplicationInstance;
import com.datastax.oss.sga.api.model.Connection;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.TopicDefinition;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is the implementation of an application instance.
 */
@Slf4j
@Data
public final class PhysicalApplicationInstance {

    private final Map<TopicDefinition, TopicImplementation> topics = new HashMap<>();
    private final Map<String, AgentImplementation> agents = new HashMap<>();
    protected final ApplicationInstance applicationInstance;

    public PhysicalApplicationInstance(ApplicationInstance applicationInstance) {
        this.applicationInstance = applicationInstance;
    }

    /**
     * Get reference to the source application instance
     * @return the source application definition
     */
    public ApplicationInstance getApplicationInstance() {
        return applicationInstance;
    }

    /**
     * Get a connection
     * @param module
     * @param connection
     * @return the connection implementation
     */
    public ConnectionImplementation getConnectionImplementation(Module module, Connection connection) {
        Connection.Connectable endpoint = connection.endpoint();
        switch (endpoint.getConnectableType()) {
            case Connection.Connectables.AGENT:
                return getAgentImplementation(module, ((AgentConfiguration) endpoint).getId());
            case Connection.Connectables.TOPIC:
                return getTopicByName(((TopicDefinition) endpoint).getName());
            default:
                throw new IllegalArgumentException("Unknown connectable type " + endpoint.getConnectableType());
        }
    }

    /**
     * Get all the Logical Topics to be deployed on the StreamingCluster
     * @return the topics to be deployed on the StreamingCluster
     */
    public List<TopicImplementation> getLogicalTopics() {
        return new ArrayList<>(topics.values());
    }

    /**
     * Register the implementation of a topic
     * @param topicDefinition
     * @param topicImplementation
     */
    public void registerTopic(TopicDefinition topicDefinition, TopicImplementation topicImplementation) {
        topics.put(topicDefinition, topicImplementation);
    }

    /**
     * Discard a topic implementation
     * @param topicImplementation
     */
    public void discardTopic(ConnectionImplementation topicImplementation) {
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
    public AgentImplementation getAgentImplementation(Module module, String id) {
        return agents.get(module.getId() + "#" + id);
    }

    public void registerAgent(Module module, String id, AgentImplementation agentImplementation) {
        String internalId = module.getId() + "#" + id;
        log.info("registering agent {} for module {} with id {}", agentImplementation, module.getId(), id);
        agents.put(internalId, agentImplementation);
    }

    public Map<String, AgentImplementation> getAgents() {
        return agents;
    }

    public TopicImplementation getTopicByName(String name) {
        return topics
                .entrySet()
                .stream()
                .filter(e -> e.getKey().getName().equals(name))
                .findFirst()
                .map(Map.Entry::getValue)
                .orElse(null);
    }
}
