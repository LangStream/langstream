package com.datastax.oss.sga.pulsar;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.model.Connection;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.SchemaDefinition;
import com.datastax.oss.sga.api.model.TopicDefinition;
import com.datastax.oss.sga.api.runtime.AgentImplementation;
import com.datastax.oss.sga.api.runtime.ConnectionImplementation;
import com.datastax.oss.sga.api.runtime.PhysicalApplicationInstance;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Data
@Slf4j
public class PulsarPhysicalApplicationInstance implements PhysicalApplicationInstance {

    private final Map<PulsarName, PulsarTopic> topics = new HashMap<>();
    private final Map<String, AgentImplementation> agents = new HashMap<>();

    private final String defaultTenant;
    private final String defaultNamespace;

    public PulsarTopic registerTopic(String tenant, String namespace, String name, SchemaDefinition schema, String creationMode) {
        PulsarName topicName
                = new PulsarName(tenant, namespace, name);
        String schemaType = schema != null ? schema.type() : null;
        String schemaDefinition = schema != null ? schema.schema() : null;
        String schemaName =  schema != null ? schema.name() : null;
        PulsarTopic pulsarTopic = new PulsarTopic(topicName,
                schemaName,
                schemaType,
                schemaDefinition,
                creationMode);
        topics.put(topicName, pulsarTopic);
        return pulsarTopic;
    }

    @Override
    public ConnectionImplementation getConnectionImplementation(Module module, Connection connection) {
        Connection.Connectable endpoint = connection.endpoint();
        if (endpoint instanceof TopicDefinition topicDefinition) {
            // compare only by name (without tenant/namespace)
            PulsarTopic pulsarTopic = topics.values()
                    .stream()
                    .filter(p -> p.name().name().equals(topicDefinition.name()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Topic " + topicDefinition.name() + " not found, only " + topics));
            return pulsarTopic;
        } else if (endpoint instanceof AgentConfiguration agentConfiguration) {
            // connecting two agents requires an intermediate topic
            String name = "agent-" + agentConfiguration.getId() + "-output";
            log.info("Automatically creating topic {} in order to connect agent {}", name,
                    agentConfiguration.getId());
            PulsarTopic pulsarTopic = registerTopic(getDefaultTenant(), getDefaultNamespace(),
                    name, null, TopicDefinition.CREATE_MODE_CREATE_IF_NOT_EXISTS);
            return pulsarTopic;
        }
        throw new UnsupportedOperationException("Not implemented yet, connection with " + endpoint);
    }

    @Override
    public AgentImplementation getAgentImplementation(Module module, String id) {
        return agents.get(module.getId() + "#" + id);
    }

    public void registerAgent(Module module, String id, AgentImplementation agentImplementation) {
        String internalId = module.getId() + "#" + id;
        log.info("registering agent {} for module {} with id {}", agentImplementation, module.getId(), id);
        agents.put(internalId, agentImplementation);
    }

}
