package com.datastax.oss.sga.impl.common;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.Pipeline;
import com.datastax.oss.sga.api.model.TopicDefinition;
import com.datastax.oss.sga.api.runtime.AgentNode;
import com.datastax.oss.sga.api.runtime.AgentNodeProvider;
import com.datastax.oss.sga.api.runtime.ComputeClusterRuntime;
import com.datastax.oss.sga.api.runtime.Connection;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.PluginsRegistry;
import com.datastax.oss.sga.api.runtime.StreamingClusterRuntime;
import com.datastax.oss.sga.api.runtime.Topic;
import lombok.extern.slf4j.Slf4j;

import java.util.stream.Collectors;

/**
 * Basic class with common utility methods for a ClusterRuntime.
 */
@Slf4j
public abstract class BasicClusterRuntime implements ComputeClusterRuntime {

    private static final int DEFAULT_PARTITIONS_FOR_IMPLICIT_TOPICS = 0;

    @Override
    public ExecutionPlan buildExecutionPlan(String applicationId,
                                            Application application,
                                            PluginsRegistry pluginsRegistry, StreamingClusterRuntime streamingClusterRuntime) {

        ExecutionPlan result =
                new ExecutionPlan(applicationId, application);

        detectTopics(result, streamingClusterRuntime);

        detectAgents(result, streamingClusterRuntime, pluginsRegistry);


        return result;
    }

    /**
     * Detects topics that are explicitly defined in the application instance.
     * @param result
     * @param streamingClusterRuntime
     */
    protected void detectTopics(ExecutionPlan result,
                                StreamingClusterRuntime streamingClusterRuntime) {
        Application applicationInstance = result.getApplication();
        for (Module module : applicationInstance.getModules().values()) {
            for (TopicDefinition topic : module.getTopics().values()) {
                Topic topicImplementation = streamingClusterRuntime.createTopicImplementation(topic, result);
                result.registerTopic(topic, topicImplementation);
            }
        }
    }

    /**
     * Detects the Agaents and build connections to them.
     * This operation may implicitly declare additional topics.
     * @param result
     * @param streamingClusterRuntime
     * @param pluginsRegistry
     */

    protected void detectAgents(ExecutionPlan result,
                                StreamingClusterRuntime streamingClusterRuntime,
                                PluginsRegistry pluginsRegistry) {
        Application applicationInstance = result.getApplication();
        for (Module module : applicationInstance.getModules().values()) {
            if (module.getPipelines() == null) {
                return;
            }
            for (Pipeline pipeline : module.getPipelines().values()) {
                log.info("Pipeline: {}", pipeline.getName());
                AgentNode previousAgent = null;
                for (AgentConfiguration agentConfiguration : pipeline.getAgents()) {
                    previousAgent = buildAgent(module, agentConfiguration, result, pluginsRegistry,
                            streamingClusterRuntime, previousAgent);
                }
            }
        }
    }


    protected AgentNode buildAgent(Module module, AgentConfiguration agentConfiguration,
                                   ExecutionPlan result,
                                   PluginsRegistry pluginsRegistry,
                                   StreamingClusterRuntime streamingClusterRuntime,
                                   AgentNode previousAgent) {
        log.info("Processing agent {} id={} type={}", agentConfiguration.getName(), agentConfiguration.getId(),
                agentConfiguration.getType());
        AgentNodeProvider agentImplementationProvider =
                pluginsRegistry.lookupAgentImplementation(agentConfiguration.getType(), this);

        AgentNode agentImplementation = agentImplementationProvider
                .createImplementation(agentConfiguration, module, result, this, pluginsRegistry, streamingClusterRuntime );

        if (previousAgent != null && agentImplementationProvider.canMerge(previousAgent, agentImplementation)) {
            agentImplementation = agentImplementationProvider.mergeAgents(previousAgent, agentImplementation, result);
        } else {
            result.registerAgent(module, agentConfiguration.getId(), agentImplementation);
        }

        return agentImplementation;
    }

    @Override
    public Connection getConnectionImplementation(Module module, com.datastax.oss.sga.api.model.Connection connection,
                                                  ExecutionPlan physicalApplicationInstance,
                                                  StreamingClusterRuntime streamingClusterRuntime) {
        com.datastax.oss.sga.api.model.Connection.Connectable endpoint = connection.endpoint();
        if (endpoint instanceof TopicDefinition topicDefinition) {
            // compare by name
            Connection result =
                    physicalApplicationInstance.getTopicByName(topicDefinition.getName());
            if (result == null) {
                throw new IllegalArgumentException("Topic " + topicDefinition.getName() + " not found, " +
                        "only " + physicalApplicationInstance.getTopics().keySet()
                        .stream()
                        .map(TopicDefinition::getName)
                        .collect(Collectors.toList()) + " are available");
            }
            return result;
        } else if (endpoint instanceof AgentConfiguration agentConfiguration) {
            return buildImplicitTopicForAgent(physicalApplicationInstance, agentConfiguration, streamingClusterRuntime);
        }
        throw new UnsupportedOperationException("Not implemented yet, connection with " + endpoint);
    }

    protected Connection buildImplicitTopicForAgent(ExecutionPlan physicalApplicationInstance,
                                                    AgentConfiguration agentConfiguration,
                                                    StreamingClusterRuntime streamingClusterRuntime) {
        // connecting two agents requires an intermediate topic
        String name = "agent-" + agentConfiguration.getId() + "-output";
        log.info("Automatically creating topic {} in order to connect agent {}", name, agentConfiguration.getId());
        // short circuit...the Pulsar Runtime works only with Pulsar Topics on the same Pulsar Cluster
        String creationMode = TopicDefinition.CREATE_MODE_CREATE_IF_NOT_EXISTS;
        TopicDefinition topicDefinition = new TopicDefinition(name, creationMode, DEFAULT_PARTITIONS_FOR_IMPLICIT_TOPICS, null, null);
        Topic topicImplementation = streamingClusterRuntime.createTopicImplementation(topicDefinition, physicalApplicationInstance);
        physicalApplicationInstance.registerTopic(topicDefinition, topicImplementation);

        return topicImplementation;
    }

    @Override
    public Object deploy(String tenant, ExecutionPlan applicationInstance, StreamingClusterRuntime streamingClusterRuntime) {
        streamingClusterRuntime.deploy(applicationInstance);
        log.warn("ClusterType " + getClusterType() + " doesn't actually deploy agents, it's just a logical representation");
        return null;
    }

    @Override
    public void delete(String tenant, ExecutionPlan applicationInstance, StreamingClusterRuntime streamingClusterRuntime) {
        streamingClusterRuntime.delete(applicationInstance);
        log.warn("ClusterType " + getClusterType() + " doesn't actually deploy agents, it's just a logical representation");
    }
}
