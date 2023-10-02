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
import ai.langstream.api.runtime.ComponentType;
import ai.langstream.impl.common.AbstractAgentProvider;
import ai.langstream.impl.noop.NoOpComputeClusterRuntimeProvider;
import ai.langstream.runtime.impl.k8s.KubernetesClusterRuntime;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Set;

public class KafkaConnectAgentsProvider extends AbstractAgentProvider {

    public KafkaConnectAgentsProvider() {
        super(
                Set.of("sink", "source"),
                List.of(
                        KubernetesClusterRuntime.CLUSTER_TYPE,
                        NoOpComputeClusterRuntimeProvider.CLUSTER_TYPE));
    }

    @Override
    protected ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        return switch (agentConfiguration.getType()) {
            case "sink" -> ComponentType.SINK;
            case "source" -> ComponentType.SOURCE;
            default -> throw new IllegalStateException();
        };
    }

    @Override
    protected boolean isAgentConfigModelAllowUnknownProperties(String type) {
        return true;
    }

    @Override
    protected Class getAgentConfigModelClass(String type) {
        return switch (type) {
            case "sink" -> KafkaSinkConnectAgentConfig.class;
            case "source" -> KafkaSourceConnectAgentConfig.class;
            default -> throw new IllegalStateException();
        };
    }

    @AgentConfig(
            name = "Kafka Connect Sink agent",
            description =
                    """
                Run any Kafka Connect Sink.
                All the configuration properties are passed to the Kafka Connect Sink.
            """)
    public static class KafkaSinkConnectAgentConfig {
        @ConfigProperty(
                description =
                        """
                                Java main class for the Kafka Sink connector.
                                """,
                required = true)
        @JsonProperty("connector.class")
        private String connectorClass;
    }

    @AgentConfig(
            name = "Kafka Connect Source agent",
            description =
                    """
                Run any Kafka Connect Source.
                All the configuration properties are passed to the Kafka Connect Source.
            """)
    public static class KafkaSourceConnectAgentConfig {
        @ConfigProperty(
                description =
                        """
                                Java main class for the Kafka Source connector.
                                """,
                required = true)
        @JsonProperty("connector.class")
        private String connectorClass;
    }
}
