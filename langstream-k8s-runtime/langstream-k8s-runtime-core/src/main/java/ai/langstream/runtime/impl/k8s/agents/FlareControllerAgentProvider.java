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

@Slf4j
public class FlareControllerAgentProvider extends AbstractComposableAgentProvider {

    private static final Set<String> SUPPORTED_AGENT_TYPES = Set.of("flare-controller");

    public FlareControllerAgentProvider() {
        super(SUPPORTED_AGENT_TYPES, List.of(KubernetesClusterRuntime.CLUSTER_TYPE, "none"));
    }

    @Override
    protected ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        return ComponentType.PROCESSOR;
    }

    @Override
    protected Class getAgentConfigModelClass(String type) {
        return FlareControllerConfiguration.class;
    }

    @AgentConfig(
            name = "Flare Controller",
            description =
                    """
            Apply to the Flare pattern to enhance the quality of text completion results.
            """)
    @Data
    public static class FlareControllerConfiguration {
        @ConfigProperty(
                description =
                        """
                    The field that contains the list of tokens returned by the ai-text-completion agent.
                                """,
                required = true)
        @JsonProperty("tokens-field")
        private String tokensField;

        @ConfigProperty(
                description =
                        """
                    The field that contains the logprobs of tokens returned by the ai-text-completion agent.
                                """,
                required = true)
        @JsonProperty("logprobs-field")
        private String logprobsField;

        @ConfigProperty(
                description =
                        """
                   Name of the topic to forward the message in case of requesting more documents.
                                """,
                required = true)
        @JsonProperty("loop-topic")
        private String loopTopic;

        @ConfigProperty(
                description =
                        """
                   Name of the field to set in order to request the retrival of more documents.
                                """,
                required = true)
        @JsonProperty("retrieve-documents-field")
        private String retrieveDocumentsField;
    }
}
