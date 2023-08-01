package com.datastax.oss.sga.impl.common;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.Pipeline;
import com.datastax.oss.sga.api.model.TopicDefinition;
import com.datastax.oss.sga.api.runtime.AgentNode;
import com.datastax.oss.sga.api.runtime.AgentNodeProvider;
import com.datastax.oss.sga.api.runtime.ComputeClusterRuntime;
import com.datastax.oss.sga.api.runtime.ConnectionImplementation;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.ExecutionPlanOptimiser;
import com.datastax.oss.sga.api.runtime.PluginsRegistry;
import com.datastax.oss.sga.api.runtime.StreamingClusterRuntime;
import com.datastax.oss.sga.api.runtime.Topic;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
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
                    previousAgent = buildAgent(module, pipeline, agentConfiguration, result, pluginsRegistry,
                            streamingClusterRuntime, previousAgent);
                }
            }
        }
    }


    protected AgentNode buildAgent(Module module,
                                   Pipeline pipeline,
                                   AgentConfiguration agentConfiguration,
                                   ExecutionPlan result,
                                   PluginsRegistry pluginsRegistry,
                                   StreamingClusterRuntime streamingClusterRuntime,
                                   AgentNode previousAgent) {
        log.info("Processing agent {} id={} type={}", agentConfiguration.getName(), agentConfiguration.getId(),
                agentConfiguration.getType());
        AgentNodeProvider agentImplementationProvider =
                pluginsRegistry.lookupAgentImplementation(agentConfiguration.getType(), this);

        AgentNode agentImplementation = agentImplementationProvider
                .createImplementation(agentConfiguration, module, pipeline, result, this, pluginsRegistry, streamingClusterRuntime );

        boolean merged = false;
        if (previousAgent != null) {

            boolean consecutiveAgentsWithImplictTopic = false;
            if (agentImplementation instanceof DefaultAgentNode agent2
                    && previousAgent instanceof DefaultAgentNode agent1) {

                ConnectionImplementation agent1OutputConnectionImplementation = agent1.getOutputConnectionImplementation();
                if (agent1OutputConnectionImplementation.equals(agent2.getInputConnectionImplementation())
                    && agent1OutputConnectionImplementation instanceof Topic topic
                    && topic.implicit()) {
                    log.info("Agent {} Output connection is {}", agent1.getId(), agent1OutputConnectionImplementation);
                    log.info("Agent {} Input connection is {}", agent2.getId(), agent2.getInputConnectionImplementation());
                    log.info("Agent {} the two agents are consecutive with an implicit topic in the pipeline", agentConfiguration.getId());

                    consecutiveAgentsWithImplictTopic = true;
                } else {
                    log.info("Agent {} Output connection is {}", agent1.getId(), agent1OutputConnectionImplementation);
                    log.info("Agent {} Input connection is {}", agent2.getId(), agent2.getInputConnectionImplementation());
                    log.info("Agent {} the two agents are NOT consecutive in the pipeline", agentConfiguration.getId());
                    consecutiveAgentsWithImplictTopic = false;
                }
            }

            if (consecutiveAgentsWithImplictTopic) {
                for (ExecutionPlanOptimiser optimiser : getExecutionPlanOptimisers()) {
                    if (optimiser.canMerge(previousAgent, agentImplementation)) {
                        agentImplementation = optimiser.mergeAgents(module, pipeline, previousAgent, agentImplementation, result);
                        merged = true;
                        break;
                    }
                }
            }

        }
        if (!merged) {
            result.registerAgent(module, agentConfiguration.getId(), agentImplementation);
        }
        return agentImplementation;
    }

    @Override
    public ConnectionImplementation getConnectionImplementation(Module module, Pipeline pipeline,
                                                                com.datastax.oss.sga.api.model.Connection connection,
                                                                ConnectionImplementation.ConnectionDirection direction,
                                                                ExecutionPlan physicalApplicationInstance,
                                                                StreamingClusterRuntime streamingClusterRuntime) {
        return switch (connection.connectionType()) {
            case TOPIC -> {
                // compare by name
                Topic result =
                        physicalApplicationInstance.getTopicByName(connection.definition());
                if (result == null) {
                    throw new IllegalArgumentException("Topic " + connection.definition() + " not found, " +
                            "only " + physicalApplicationInstance.getTopics().keySet()
                            .stream()
                            .map(TopicDefinition::getName)
                            .collect(Collectors.toList()) + " are available");
                }

                if (connection.enableDeadletterQueue()) {
                    TopicDefinition topicDefinition = module.getTopics().get(result.topicName());
                    Topic deadLetterTopic = buildImplicitTopicForDeadletterQueue(result, topicDefinition, streamingClusterRuntime, physicalApplicationInstance);
                    result.bindDeadletterTopic(deadLetterTopic);
                }

                yield result;
            }
            case AGENT -> {
                AgentConfiguration agentConfiguration = pipeline
                        .getAgent(connection.definition());
                yield switch (direction) {
                    case OUTPUT -> {
                        ConnectionImplementation result = buildImplicitTopicForAgent(physicalApplicationInstance, agentConfiguration,  streamingClusterRuntime);
                        yield result;
                    }
                    case INPUT -> {
                        if (agentConfiguration.getOutput() != null) {
                            yield getConnectionImplementation(module, pipeline, agentConfiguration.getOutput(), ConnectionImplementation.ConnectionDirection.OUTPUT, physicalApplicationInstance, streamingClusterRuntime);
                        }
                        throw new IllegalStateException("Invalid agent configuration for (" + agentConfiguration.getName() + ") , missing output");
                    }
                };
            }
        };
    }

    private Topic buildImplicitTopicForDeadletterQueue(Topic connection, TopicDefinition inputTopicDefinition, StreamingClusterRuntime streamingClusterRuntime,
                                                      ExecutionPlan physicalApplicationInstance) {
        // connecting two agents requires an intermediate topic
        String name = connection.topicName() + "-deadletter";
        log.info("Automatically creating deadletter topic {} for topic {}", name, connection.topicName());
        // short circuit...the Pulsar Runtime works only with Pulsar Topics on the same Pulsar Cluster
        String creationMode = TopicDefinition.CREATE_MODE_CREATE_IF_NOT_EXISTS;
        // we keep the same schemas as the input topic
        TopicDefinition topicDefinition = new TopicDefinition(name, creationMode,
                inputTopicDefinition.isImplicit(),
                inputTopicDefinition.getPartitions(),
                inputTopicDefinition.getKeySchema(),
                inputTopicDefinition.getValueSchema(),
                Map.of(), Map.of());
        Topic topicImplementation = streamingClusterRuntime.createTopicImplementation(topicDefinition, physicalApplicationInstance);
        physicalApplicationInstance.registerTopic(topicDefinition, topicImplementation);
        return topicImplementation;
    }

    protected ConnectionImplementation buildImplicitTopicForAgent(ExecutionPlan physicalApplicationInstance,
                                                                  AgentConfiguration agentConfiguration,
                                                                  StreamingClusterRuntime streamingClusterRuntime) {
        // connecting two agents requires an intermediate topic
        String name = "agent-" + agentConfiguration.getId() + "-input";
        log.info("Automatically creating topic {} in order to connect as input for agent {}", name, agentConfiguration.getId());
        // short circuit...the Pulsar Runtime works only with Pulsar Topics on the same Pulsar Cluster
        String creationMode = TopicDefinition.CREATE_MODE_CREATE_IF_NOT_EXISTS;
        TopicDefinition topicDefinition = new TopicDefinition(name, creationMode, true, DEFAULT_PARTITIONS_FOR_IMPLICIT_TOPICS, null, null,
                Map.of(), Map.of());
        Topic topicImplementation = streamingClusterRuntime.createTopicImplementation(topicDefinition, physicalApplicationInstance);
        physicalApplicationInstance.registerTopic(topicDefinition, topicImplementation);

        return topicImplementation;
    }

    @Override
    public Object deploy(String tenant, ExecutionPlan applicationInstance, StreamingClusterRuntime streamingClusterRuntime,
                         String codeStorageArchiveId) {
        streamingClusterRuntime.deploy(applicationInstance);
        log.warn("ClusterType " + getClusterType() + " doesn't actually deploy agents, it's just a logical representation");
        return null;
    }

    @Override
    public void delete(String tenant, ExecutionPlan applicationInstance, StreamingClusterRuntime streamingClusterRuntime,
                       String codeStorageArchiveId) {
        streamingClusterRuntime.delete(applicationInstance);
        log.warn("ClusterType " + getClusterType() + " doesn't actually deploy agents, it's just a logical representation");
    }
}
