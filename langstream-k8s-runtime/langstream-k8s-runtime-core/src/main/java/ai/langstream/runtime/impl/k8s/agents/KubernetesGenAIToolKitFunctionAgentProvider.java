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

import ai.langstream.api.model.AgentConfiguration;
import ai.langstream.impl.agents.ai.GenAIToolKitFunctionAgentProvider;
import ai.langstream.runtime.impl.k8s.KubernetesClusterRuntime;
import java.util.Objects;

public class KubernetesGenAIToolKitFunctionAgentProvider extends GenAIToolKitFunctionAgentProvider {

    public KubernetesGenAIToolKitFunctionAgentProvider() {
        super(KubernetesClusterRuntime.CLUSTER_TYPE);
    }

    @Override
    protected String getAgentType(AgentConfiguration agentConfiguration) {
        // in kubernetes we keep the original agentType
        return agentConfiguration.getType();
    }

    @Override
    protected boolean isComposable(AgentConfiguration agentConfiguration) {
        return Objects.equals(
                "true",
                agentConfiguration.getConfiguration().getOrDefault("composable", true) + "");
    }
}
