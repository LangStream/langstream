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
package ai.langstream.runtime.impl.k8s.agents;

import ai.langstream.api.doc.AgentConfig;
import ai.langstream.api.doc.ConfigProperty;
import ai.langstream.api.doc.ExtendedValidationType;
import ai.langstream.api.model.AgentConfiguration;
import ai.langstream.api.model.Module;
import ai.langstream.api.model.Pipeline;
import ai.langstream.api.runtime.ComponentType;
import ai.langstream.api.runtime.ComputeClusterRuntime;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.api.util.ConfigurationUtils;
import ai.langstream.impl.agents.AbstractComposableAgentProvider;
import ai.langstream.impl.uti.ClassConfigValidator;
import ai.langstream.runtime.impl.k8s.KubernetesClusterRuntime;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/** Implements support for Flow Control Processing Agents. */
@Slf4j
public class FlowControlAgentsProvider extends AbstractComposableAgentProvider {

    protected static final String DISPATCH = "dispatch";
    protected static final String TIMER_SOURCE = "timer-source";
    protected static final String TRIGGER_EVENT = "trigger-event";

    protected static final String LOG_EVENT = "log-event";
    private static final Set<String> SUPPORTED_AGENT_TYPES =
            Set.of(DISPATCH, TIMER_SOURCE, TRIGGER_EVENT, LOG_EVENT);

    public FlowControlAgentsProvider() {
        super(SUPPORTED_AGENT_TYPES, List.of(KubernetesClusterRuntime.CLUSTER_TYPE, "none"));
    }

    @Override
    protected ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        return switch (agentConfiguration.getType()) {
            case DISPATCH -> ComponentType.PROCESSOR;
            case TIMER_SOURCE -> ComponentType.SOURCE;
            case TRIGGER_EVENT -> ComponentType.PROCESSOR;
            case LOG_EVENT -> ComponentType.PROCESSOR;
            default -> throw new IllegalArgumentException(
                    "Unsupported agent type: " + agentConfiguration.getType());
        };
    }

    @Override
    protected Map<String, Object> computeAgentConfiguration(
            AgentConfiguration agentConfiguration,
            Module module,
            Pipeline pipeline,
            ExecutionPlan executionPlan,
            ComputeClusterRuntime clusterRuntime,
            PluginsRegistry pluginsRegistry) {
        switch (agentConfiguration.getType()) {
            case DISPATCH:
                {
                    DispatchConfig dispatchConfig =
                            ClassConfigValidator.convertValidatedConfiguration(
                                    agentConfiguration.getConfiguration(), DispatchConfig.class);
                    List<RouteConfiguration> routes = dispatchConfig.getRoutes();
                    if (routes != null) {
                        for (RouteConfiguration routeConfiguration : routes) {
                            String action = routeConfiguration.getAction();
                            ConfigurationUtils.validateEnumValue(
                                    "action",
                                    Set.of("dispatch", "drop"),
                                    action,
                                    () -> "route " + routeConfiguration);
                            String destination = routeConfiguration.getDestination();
                            if (destination != null && !destination.isEmpty()) {
                                if (action.equals("drop")) {
                                    throw new IllegalArgumentException(
                                            "drop action cannot have a destination");
                                }
                                log.info("Validating topic reference {}", destination);
                                module.resolveTopic(destination);
                            }
                        }
                    }
                    break;
                }
            case TRIGGER_EVENT:
                {
                    TriggerEventProcessorConfig triggerEventProcessorConfig =
                            ClassConfigValidator.convertValidatedConfiguration(
                                    agentConfiguration.getConfiguration(),
                                    TriggerEventProcessorConfig.class);
                    String destination = triggerEventProcessorConfig.getDestination();
                    if (destination != null && !destination.isEmpty()) {
                        log.info("Validating topic reference {}", destination);
                        module.resolveTopic(destination);
                    }
                    break;
                }
        }
        return super.computeAgentConfiguration(
                agentConfiguration,
                module,
                pipeline,
                executionPlan,
                clusterRuntime,
                pluginsRegistry);
    }

    @Override
    protected Class getAgentConfigModelClass(String type) {
        return switch (type) {
            case DISPATCH -> DispatchConfig.class;
            case TIMER_SOURCE -> TimeSourceConfig.class;
            case TRIGGER_EVENT -> TriggerEventProcessorConfig.class;
            case LOG_EVENT -> LogEventProcessorConfig.class;
            default -> throw new IllegalArgumentException("Unsupported agent type: " + type);
        };
    }

    @AgentConfig(
            name = "Timer source",
            description =
                    """
            Periodically emits records to trigger the execution of pipelines.
            """)
    @Data
    public static class TimeSourceConfig {
        @ConfigProperty(
                description =
                        """
                        Fields of the generated records.
                                """)
        List<FieldConfiguration> fields;

        @ConfigProperty(
                description =
                        """
                        Period of the timer in seconds.
                                """,
                defaultValue = "60")
        @JsonProperty("period-seconds")
        int periodInSeconds;
    }

    @AgentConfig(
            name = "Log an event",
            description =
                    """
            Log a line in the agent logs when a record is received.
            """)
    @Data
    public static class LogEventProcessorConfig {
        @ConfigProperty(
                description =
                        """
                        Fields to log.
                                """)
        List<FieldConfiguration> fields;

        @ConfigProperty(
                description =
                        """
                        Template for a log message to print (Mustache).
                                """)
        String message;

        @ConfigProperty(
                description =
                        """
                        Condition to trigger the operation. This is a standard EL expression.
                                """,
                defaultValue = "true",
                extendedValidationType = ExtendedValidationType.EL_EXPRESSION)
        @JsonProperty("when")
        String when;
    }

    @AgentConfig(
            name = "Trigger event",
            description =
                    """
            Emits a record on a side destination when a record is received.
            """)
    @Data
    public static class TriggerEventProcessorConfig {
        @ConfigProperty(
                description =
                        """
                        Fields of the generated records.
                                """)
        List<FieldConfiguration> fields;

        @ConfigProperty(
                description =
                        """
                        Condition to trigger the event. This is a standard EL expression.
                                """,
                defaultValue = "true",
                extendedValidationType = ExtendedValidationType.EL_EXPRESSION)
        @JsonProperty("when")
        String when;

        @ConfigProperty(
                description =
                        """
                        Whether to continue processing the record downstream after emitting the event.
                        If the when condition is false, the record is passed downstream anyway.
                        This flag allows you to stop processing system events and trigger a different pipeline.
                                """,
                defaultValue = "true")
        @JsonProperty("continue-processing")
        boolean continueProcessing;

        @ConfigProperty(
                description =
                        """
                        Destination of the message.
                                """,
                defaultValue = "",
                required = true)
        @JsonProperty("destination")
        String destination;
    }

    @AgentConfig(
            name = "Dispatch agent",
            description =
                    """
            Dispatches messages to different destinations based on conditions.
            """)
    @Data
    public static class DispatchConfig {
        @ConfigProperty(
                description =
                        """
                        Routes.
                                """)
        List<RouteConfiguration> routes;
    }

    @Data
    public static class RouteConfiguration {
        @ConfigProperty(
                description =
                        """
                        Condition to activate the route. This is a standard EL expression.
                                """,
                extendedValidationType = ExtendedValidationType.EL_EXPRESSION)
        String when;

        @ConfigProperty(
                description =
                        """
                        Destination of the message.
                        """)
        String destination;

        @ConfigProperty(
                description =
                        """
                        Action on the message. Possible values are "dispatch" or "drop".
                        """,
                defaultValue = "dispatch")
        String action = "dispatch";
    }

    @Data
    public static class FieldConfiguration {
        @ConfigProperty(
                description =
                        """
                        Name of the field like value.xx, key.xxx, properties.xxx
                                """,
                required = true)
        String name;

        @ConfigProperty(
                description =
                        """
                        Expression to compute the value of the field. This is a standard EL expression.
                                """,
                required = true,
                extendedValidationType = ExtendedValidationType.EL_EXPRESSION)
        String expression;
    }
}
