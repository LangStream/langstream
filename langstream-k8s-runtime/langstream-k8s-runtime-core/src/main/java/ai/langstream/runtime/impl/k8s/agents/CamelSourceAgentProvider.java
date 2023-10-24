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
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Set;

/** Implements support for Apache Camel based Source Agents. */
@Slf4j
public class CamelSourceAgentProvider extends AbstractComposableAgentProvider {

    protected static final String CAMEL_SOURCE = "camel-source";

    public CamelSourceAgentProvider() {
        super(
                Set.of(CAMEL_SOURCE),
                List.of(KubernetesClusterRuntime.CLUSTER_TYPE, "none"));
    }

    @Override
    protected final ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        return ComponentType.SOURCE;
    }

    @Override
    protected Class getAgentConfigModelClass(String type) {
        return CamelSourceConfiguration.class;
    }

    @AgentConfig(name = "Apache Camel Source", description = "Use Apache Camel components as Source")
    @Data
    public static class CamelSourceConfiguration {


        @ConfigProperty(
                description =
                        """
                        The Camel URI of the component to use as Source.
                        """,
                defaultValue = "", required = true)
        @JsonProperty("component-uri")
        private String componentUri;
    }
}
