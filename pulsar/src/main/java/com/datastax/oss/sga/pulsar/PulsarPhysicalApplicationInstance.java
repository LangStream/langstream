package com.datastax.oss.sga.pulsar;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.model.ApplicationInstance;
import com.datastax.oss.sga.api.model.Connection;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.TopicDefinition;
import com.datastax.oss.sga.api.runtime.ConnectionImplementation;
import com.datastax.oss.sga.api.runtime.PhysicalApplicationInstance;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class PulsarPhysicalApplicationInstance extends PhysicalApplicationInstance {

    @Getter
    @Setter
    private final String defaultTenant;
    @Getter
    @Setter
    private final String defaultNamespace;

    public PulsarPhysicalApplicationInstance(ApplicationInstance applicationInstance, String defaultTenant, String defaultNamespace) {
        super(applicationInstance);
        this.defaultTenant = defaultTenant;
        this.defaultNamespace = defaultNamespace;
    }

    @Override
    public ConnectionImplementation getConnectionImplementation(Module module, Connection connection) {
        Connection.Connectable endpoint = connection.endpoint();
        if (endpoint instanceof TopicDefinition topicDefinition) {
            // compare only by name (without tenant/namespace)
            PulsarTopic pulsarTopic = getLogicalTopics()
                    .stream()
                    .map(p -> (PulsarTopic) p)
                    .filter(p -> p.name().name().equals(topicDefinition.getName()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Topic " + topicDefinition.getName() + " not found, only " + getLogicalTopics()));
            return pulsarTopic;
        } else if (endpoint instanceof AgentConfiguration agentConfiguration) {
            // connecting two agents requires an intermediate topic
            String name = "agent-" + agentConfiguration.getId() + "-output";
            log.info("Automatically creating topic {} in order to connect agent {}", name,
                    agentConfiguration.getId());
            // short circuit...the Pulsar Runtime works only with Pulsar Topics on the same Pulsar Cluster
            String creationMode = TopicDefinition.CREATE_MODE_CREATE_IF_NOT_EXISTS;
            TopicDefinition topicDefinition = new TopicDefinition(name, creationMode, null);
            PulsarName pulsarName = new PulsarName(defaultTenant, defaultNamespace, name);
            PulsarTopic pulsarTopic = new PulsarTopic(pulsarName, null, null, null, creationMode);
            registerTopic(topicDefinition, pulsarTopic);
            return pulsarTopic;
        }
        throw new UnsupportedOperationException("Not implemented yet, connection with " + endpoint);
    }

}
