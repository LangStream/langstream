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
import ai.langstream.impl.agents.AbstractComposableAgentProvider;
import ai.langstream.runtime.impl.k8s.KubernetesClusterRuntime;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Set;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/** Implements support for Apache Camel based Source Agents. */
@Slf4j
public class PulsarDLQSourceAgentProvider extends AbstractComposableAgentProvider {

    protected static final String PULSAR_DLQ_SOURCE = "pulsardlq-source";

    public PulsarDLQSourceAgentProvider() {
        super(Set.of(PULSAR_DLQ_SOURCE), List.of(KubernetesClusterRuntime.CLUSTER_TYPE, "none"));
    }

    @Override
    protected final ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        return ComponentType.SOURCE;
    }

    @Override
    protected Class getAgentConfigModelClass(String type) {
        return PulsarDLQSourceConfiguration.class;
    }

    @AgentConfig(
            name = "Pulsar DLQ Source",
            description = "Listen to all DLQ topics in a namespace in Pulsar")
    @Data
    public static class PulsarDLQSourceConfiguration {

        @ConfigProperty(
                description =
                        """
                        The URL of the Pulsar cluster to connect to.
                        """,
                defaultValue = "pulsar://localhost:6650",
                required = true)
        @JsonProperty("pulsar-url")
        private String pulsarUrl;

        @ConfigProperty(
                description =
                        """
                        Namespace to listen for DLQ topics.
                        """,
                defaultValue = "public/default",
                required = true)
        @JsonProperty("namespace")
        private String namespace;

        @ConfigProperty(
                description =
                        """
                        Subscription name to use for the DLQ topics.
                        """,
                defaultValue = "langstream-dlq-subscription",
                required = true)
        @JsonProperty("subscription")
        private String subscription;

        @ConfigProperty(
                description =
                        """
                        Suffix to use for DLQ topics.
                        """,
                defaultValue = "-DLQ",
                required = true)
        @JsonProperty("dlq-suffix")
        private String dlqSuffix;

        @ConfigProperty(
                description =
                        """
                        Include partitioned topics in the DLQ topics.
                        """,
                defaultValue = "false")
        @JsonProperty("include-partitioned")
        private boolean includePartitioned;
    }
}
