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
public class ReRankAgentProvider extends AbstractComposableAgentProvider {

    private static final Set<String> SUPPORTED_AGENT_TYPES = Set.of("re-rank");

    public ReRankAgentProvider() {
        super(SUPPORTED_AGENT_TYPES, List.of(KubernetesClusterRuntime.CLUSTER_TYPE, "none"));
    }

    @Override
    protected ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        return ComponentType.PROCESSOR;
    }

    @Override
    protected Class getAgentConfigModelClass(String type) {
        return ReRankConfiguration.class;
    }

    @AgentConfig(
            name = "Re-rank",
            description =
                    """
            Agent for re-ranking documents based on a query.
            """)
    @Data
    public static class ReRankConfiguration {
        @ConfigProperty(
                description =
                        """
                    The field that contains the documents to sort.
                                """,
                required = true)
        private String field;

        @ConfigProperty(
                description =
                        """
                    The field that will hold the results, it can be the same as "field" to override it.
                                """,
                required = true)
        @JsonProperty("output-field")
        private String outputField;

        @ConfigProperty(
                description =
                        """
                    Algorithm to use for re-ranking. 'none' or 'MMR'.
                                """,
                defaultValue = "none")
        private String algorithm;

        @ConfigProperty(
                description =
                        """
                    Field that contains the embeddings of the documents to sort.
                                """)
        @JsonProperty("query-embeddings")
        private String queryEmbeddings;

        @ConfigProperty(
                description =
                        """
                    Field that already contains the text that has been embedded.
                                """)
        @JsonProperty("query-text")
        private String queryText;

        @ConfigProperty(
                description =
                        """
                    Result field for the embeddings.
                                """)
        @JsonProperty("embeddings-field")
        private String embeddingsField;

        @ConfigProperty(
                description =
                        """
                    Result field for the text.
                                """)
        @JsonProperty("text-field")
        private String textField;

        @ConfigProperty(
                description =
                        """
                    Maximum number of documents to keep.
                                """,
                defaultValue = "100")
        private int max;

        @ConfigProperty(
                description =
                        """
                    Parameter for MMR algorithm.
                                """,
                defaultValue = "0.5")
        private double lambda;

        @ConfigProperty(
                description =
                        """
                    Parameter for B25 algorithm.
                                """,
                defaultValue = "1.5")
        private double k1;

        @ConfigProperty(
                description =
                        """
                    Parameter for B25 algorithm.
                                """,
                defaultValue = "0.75")
        private double b;
    }
}
