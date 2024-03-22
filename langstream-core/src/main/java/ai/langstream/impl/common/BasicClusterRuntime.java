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
package ai.langstream.impl.common;

import ai.langstream.api.model.AgentConfiguration;
import ai.langstream.api.model.Application;
import ai.langstream.api.model.AssetDefinition;
import ai.langstream.api.model.Connection;
import ai.langstream.api.model.Module;
import ai.langstream.api.model.Pipeline;
import ai.langstream.api.model.Resource;
import ai.langstream.api.model.TopicDefinition;
import ai.langstream.api.runtime.AgentNode;
import ai.langstream.api.runtime.AgentNodeProvider;
import ai.langstream.api.runtime.AssetNode;
import ai.langstream.api.runtime.AssetNodeProvider;
import ai.langstream.api.runtime.ComputeClusterRuntime;
import ai.langstream.api.runtime.ConnectionImplementation;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.ExecutionPlanOptimiser;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.api.runtime.ResourceNodeProvider;
import ai.langstream.api.runtime.StreamingClusterRuntime;
import ai.langstream.api.runtime.Topic;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/** Basic class with common utility methods for a ClusterRuntime. */
@Slf4j
public abstract class BasicClusterRuntime implements ComputeClusterRuntime {

    private static final int DEFAULT_PARTITIONS_FOR_IMPLICIT_TOPICS = 0;

    @Override
    public ExecutionPlan buildExecutionPlan(
            String applicationId,
            Application application,
            PluginsRegistry pluginsRegistry,
            StreamingClusterRuntime streamingClusterRuntime) {

        if (StringUtils.isEmpty(applicationId)) {
            throw new IllegalArgumentException("Application id cannot be empty");
        }

        ExecutionPlan result = new ExecutionPlan(applicationId, application);

        detectTopics(result, streamingClusterRuntime);

        detectAssets(result, pluginsRegistry);

        detectAgents(result, streamingClusterRuntime, pluginsRegistry);

        validateExecutionPlan(result, streamingClusterRuntime);

        return result;
    }

    protected void validateExecutionPlan(
            ExecutionPlan plan, StreamingClusterRuntime streamingClusterRuntime)
            throws IllegalArgumentException {}

    /**
     * Detects topics that are explicitly defined in the application instance.
     *
     * @param result the execution plan
     * @param streamingClusterRuntime the cluster runtime
     */
    protected void detectTopics(
            ExecutionPlan result, StreamingClusterRuntime streamingClusterRuntime) {
        Application applicationInstance = result.getApplication();
        for (Module module : applicationInstance.getModules().values()) {
            for (TopicDefinition topic : module.getTopics().values()) {
                Topic topicImplementation =
                        streamingClusterRuntime.createTopicImplementation(
                                topic, result.getApplication().getInstance().streamingCluster());
                result.registerTopic(topic, topicImplementation);
            }
        }
    }

    /** Detects assets that are explicitly defined in the application instance. */
    protected void detectAssets(ExecutionPlan result, PluginsRegistry pluginsRegistry) {
        Application applicationInstance = result.getApplication();
        for (Module module : applicationInstance.getModules().values()) {
            if (module.getAssets() != null) {
                // the order is important, there may be some dependencies between them
                for (AssetDefinition asset : module.getAssets()) {
                    AssetNodeProvider assetNodeProvider =
                            pluginsRegistry.lookupAssetImplementation(asset.getAssetType(), this);
                    AssetNode assetImplementation =
                            assetNodeProvider.createImplementation(
                                    asset, module, result, this, pluginsRegistry);
                    result.registerAsset(assetImplementation);
                }
            }
        }
    }

    /**
     * Detects the Agaents and build connections to them. This operation may implicitly declare
     * additional topics.
     *
     * @param result the execution plan
     * @param streamingClusterRuntime the cluster runtime
     * @param pluginsRegistry the plugins registry
     */
    protected void detectAgents(
            ExecutionPlan result,
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
                    previousAgent =
                            buildAgent(
                                    module,
                                    pipeline,
                                    agentConfiguration,
                                    result,
                                    pluginsRegistry,
                                    streamingClusterRuntime,
                                    previousAgent);
                }
            }
        }
    }

    @Override
    public Map<String, Object> getResourceImplementation(
            Resource resource, PluginsRegistry pluginsRegistry) {
        ResourceNodeProvider nodeProvider =
                pluginsRegistry.lookupResourceImplementation(resource.type(), this);
        final Map<String, Object> newConfig =
                nodeProvider.createImplementation(resource, pluginsRegistry);
        return new HashMap<>(newConfig);
    }

    protected AgentNode buildAgent(
            Module module,
            Pipeline pipeline,
            AgentConfiguration agentConfiguration,
            ExecutionPlan result,
            PluginsRegistry pluginsRegistry,
            StreamingClusterRuntime streamingClusterRuntime,
            AgentNode previousAgent) {
        log.info(
                "Processing agent {} id={} type={}",
                agentConfiguration.getName(),
                agentConfiguration.getId(),
                agentConfiguration.getType());
        AgentNodeProvider agentImplementationProvider =
                pluginsRegistry.lookupAgentImplementation(agentConfiguration.getType(), this);

        AgentNode agentImplementation =
                agentImplementationProvider.createImplementation(
                        agentConfiguration,
                        module,
                        pipeline,
                        result,
                        this,
                        pluginsRegistry,
                        streamingClusterRuntime);

        boolean merged = false;
        if (previousAgent != null) {

            boolean consecutiveAgentsWithImplictTopic = false;
            if (agentImplementation instanceof DefaultAgentNode agent2
                    && previousAgent instanceof DefaultAgentNode agent1) {

                ConnectionImplementation agent1OutputConnectionImplementation =
                        agent1.getOutputConnectionImplementation();
                if (agent1OutputConnectionImplementation == null) {
                    throw new IllegalStateException(
                            "Invalid agent configuration for ("
                                    + agent1.getId()
                                    + "), missing output");
                }
                ;
                if (agent1OutputConnectionImplementation.equals(
                                agent2.getInputConnectionImplementation())
                        && agent1OutputConnectionImplementation instanceof Topic topic
                        && topic.implicit()) {
                    log.info(
                            "Agent {} Output connection is {}",
                            agent1.getId(),
                            agent1OutputConnectionImplementation);
                    log.info(
                            "Agent {} Input connection is {}",
                            agent2.getId(),
                            agent2.getInputConnectionImplementation());
                    log.info(
                            "Agent {} the two agents are consecutive with an implicit topic in the pipeline",
                            agentConfiguration.getId());

                    consecutiveAgentsWithImplictTopic = true;
                } else {
                    log.info(
                            "Agent {} Output connection is {}",
                            agent1.getId(),
                            agent1OutputConnectionImplementation);
                    log.info(
                            "Agent {} Input connection is {}",
                            agent2.getId(),
                            agent2.getInputConnectionImplementation());
                    log.info(
                            "Agent {} the two agents are NOT consecutive in the pipeline",
                            agentConfiguration.getId());
                }
            }

            if (consecutiveAgentsWithImplictTopic) {
                for (ExecutionPlanOptimiser optimiser : getExecutionPlanOptimisers()) {
                    if (optimiser.supports(getClusterType())) {
                        if (optimiser.canMerge(previousAgent, agentImplementation)) {
                            agentImplementation =
                                    optimiser.mergeAgents(
                                            module,
                                            pipeline,
                                            previousAgent,
                                            agentImplementation,
                                            result);
                            merged = true;
                            break;
                        }
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
    public ConnectionImplementation getConnectionImplementation(
            Module module,
            Pipeline pipeline,
            Connection connection,
            ConnectionImplementation.ConnectionDirection direction,
            ExecutionPlan physicalApplicationInstance,
            StreamingClusterRuntime streamingClusterRuntime) {
        return switch (connection.connectionType()) {
            case TOPIC -> {
                // compare by name
                Topic result = physicalApplicationInstance.getTopicByName(connection.definition());
                if (result == null) {
                    throw new IllegalArgumentException(
                            "Topic "
                                    + connection.definition()
                                    + " not found, "
                                    + "only "
                                    + physicalApplicationInstance.getTopics().keySet().stream()
                                            .map(TopicDefinition::getName)
                                            .toList()
                                    + " are available");
                }

                ensureDeadLetterTopic(
                        connection, physicalApplicationInstance, streamingClusterRuntime, result);

                yield result;
            }
            case AGENT -> {
                AgentConfiguration agentConfiguration = pipeline.getAgent(connection.definition());
                yield switch (direction) {
                    case OUTPUT -> {
                        Topic result =
                                buildImplicitTopicForAgent(
                                        physicalApplicationInstance,
                                        agentConfiguration,
                                        streamingClusterRuntime);
                        ensureDeadLetterTopic(
                                connection,
                                physicalApplicationInstance,
                                streamingClusterRuntime,
                                result);
                        yield result;
                    }
                    case INPUT -> {
                        if (agentConfiguration.getOutput() != null) {
                            yield getConnectionImplementation(
                                    module,
                                    pipeline,
                                    agentConfiguration.getOutput(),
                                    ConnectionImplementation.ConnectionDirection.OUTPUT,
                                    physicalApplicationInstance,
                                    streamingClusterRuntime);
                        }
                        throw new IllegalStateException(
                                "Invalid agent configuration for ("
                                        + agentConfiguration.getName()
                                        + ") , missing output");
                    }
                };
            }
        };
    }

    private void ensureDeadLetterTopic(
            Connection connection,
            ExecutionPlan physicalApplicationInstance,
            StreamingClusterRuntime streamingClusterRuntime,
            Topic result) {
        if (connection.enableDeadletterQueue()) {
            TopicDefinition topicDefinition =
                    physicalApplicationInstance.getTopicDefinitionByName(result.topicName());
            Topic deadLetterTopic =
                    buildImplicitTopicForDeadletterQueue(
                            result,
                            topicDefinition,
                            streamingClusterRuntime,
                            physicalApplicationInstance);
            result.bindDeadletterTopic(deadLetterTopic);
        }
    }

    private Topic buildImplicitTopicForDeadletterQueue(
            Topic connection,
            TopicDefinition inputTopicDefinition,
            StreamingClusterRuntime streamingClusterRuntime,
            ExecutionPlan executionPlan) {
        // connecting two agents requires an intermediate topic
        String name = inputTopicDefinition.getName() + "-deadletter";
        log.info(
                "Automatically creating deadletter topic {} for topic {}",
                name,
                connection.topicName());
        // short circuit...the Pulsar Runtime works only with Pulsar Topics on the same Pulsar
        // Cluster
        String creationMode = TopicDefinition.CREATE_MODE_CREATE_IF_NOT_EXISTS;
        // we keep the same schemas as the input topic
        TopicDefinition topicDefinition =
                new TopicDefinition(
                        name,
                        creationMode,
                        inputTopicDefinition.getDeletionMode(),
                        inputTopicDefinition.isImplicit(),
                        inputTopicDefinition.getPartitions(),
                        inputTopicDefinition.getKeySchema(),
                        inputTopicDefinition.getValueSchema(),
                        Map.of(),
                        Map.of());
        Topic topicImplementation =
                streamingClusterRuntime.createTopicImplementation(
                        topicDefinition,
                        executionPlan.getApplication().getInstance().streamingCluster());
        executionPlan.registerTopic(topicDefinition, topicImplementation);
        return topicImplementation;
    }

    protected Topic buildImplicitTopicForAgent(
            ExecutionPlan physicalApplicationInstance,
            AgentConfiguration agentConfiguration,
            StreamingClusterRuntime streamingClusterRuntime) {
        // connecting two agents requires an intermediate topic
        final String name = "agent-" + agentConfiguration.getId() + "-input";
        log.info(
                "Automatically creating topic {} in order to connect as input for agent {}",
                name,
                agentConfiguration.getId());
        // short circuit...the Pulsar Runtime works only with Pulsar Topics on the same Pulsar
        // Cluster
        final String creationMode = TopicDefinition.CREATE_MODE_CREATE_IF_NOT_EXISTS;
        final String deletionMode = TopicDefinition.DELETE_MODE_NONE;
        TopicDefinition topicDefinition =
                new TopicDefinition(
                        name,
                        creationMode,
                        deletionMode,
                        true,
                        DEFAULT_PARTITIONS_FOR_IMPLICIT_TOPICS,
                        null,
                        null,
                        Map.of(),
                        Map.of());
        Topic topicImplementation =
                streamingClusterRuntime.createTopicImplementation(
                        topicDefinition,
                        physicalApplicationInstance
                                .getApplication()
                                .getInstance()
                                .streamingCluster());
        physicalApplicationInstance.registerTopic(topicDefinition, topicImplementation);

        return topicImplementation;
    }
}
