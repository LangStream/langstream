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
import ai.langstream.api.runtime.ComponentType;
import ai.langstream.impl.common.AbstractAgentProvider;
import ai.langstream.runtime.impl.k8s.KubernetesClusterRuntime;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/** Implements support for custom Agents written in Python. */
@Slf4j
public class PythonCodeAgentProvider extends AbstractAgentProvider {

    public PythonCodeAgentProvider() {
        super(
                Set.of("python-source", "python-sink", "python-function"),
                List.of(KubernetesClusterRuntime.CLUSTER_TYPE, "none"));
    }

    @Override
    protected final ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        return switch (agentConfiguration.getType()) {
            case "python-source" -> ComponentType.SOURCE;
            case "python-sink" -> ComponentType.SINK;
            case "python-function" -> ComponentType.PROCESSOR;
            default -> throw new IllegalArgumentException(
                    "Unsupported agent type: " + agentConfiguration.getType());
        };
    }
}
