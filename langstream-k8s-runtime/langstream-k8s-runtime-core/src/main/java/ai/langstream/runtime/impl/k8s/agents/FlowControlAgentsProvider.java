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
import ai.langstream.api.model.AgentConfiguration;
import ai.langstream.api.model.Module;
import ai.langstream.api.model.Pipeline;
import ai.langstream.api.runtime.ComponentType;
import ai.langstream.api.runtime.ComputeClusterRuntime;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.impl.agents.AbstractComposableAgentProvider;
import ai.langstream.impl.uti.ClassConfigValidator;
import ai.langstream.runtime.impl.k8s.KubernetesClusterRuntime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/** Implements support for Flow Control Processing Agents. */
@Slf4j
public class FlowControlAgentsProvider extends AbstractComposableAgentProvider {

    protected static final String DISPATCH = "dispatch";
    private static final Set<String> SUPPORTED_AGENT_TYPES = Set.of(DISPATCH);

    public FlowControlAgentsProvider() {
        super(SUPPORTED_AGENT_TYPES, List.of(KubernetesClusterRuntime.CLUSTER_TYPE, "none"));
    }

    @Override
    protected ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        return ComponentType.PROCESSOR;
    }

    @Override
    protected Map<String, Object> computeAgentConfiguration(
            AgentConfiguration agentConfiguration,
            Module module,
            Pipeline pipeline,
            ExecutionPlan executionPlan,
            ComputeClusterRuntime clusterRuntime,
            PluginsRegistry pluginsRegistry) {
        DispatchConfig dispatchConfig =
                ClassConfigValidator.convertValidatedConfiguration(
                        agentConfiguration.getConfiguration(), DispatchConfig.class);
        List<RouteConfiguration> routes = dispatchConfig.getRoutes();
        if (routes != null) {
            for (RouteConfiguration routeConfiguration : routes) {
                String destination = routeConfiguration.getDestination();
                if (destination != null && !destination.isEmpty()) {
                    log.info("Validating topic reference {}", destination);
                    module.resolveTopic(destination);
                }
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
            default -> throw new IllegalArgumentException("Unsupported agent type: " + type);
        };
    }

    @AgentConfig(
            name = "Text extractor",
            description =
                    """
            Extracts text content from different document formats like PDF, JSON, XML, ODF, HTML and many others.
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
                        Condition to activate the route.
                                """)
        String when;

        @ConfigProperty(
                description =
                        """
                        Destination of the message. If the destination is empty the message is discarded
                        """)
        String destination;
    }
}
