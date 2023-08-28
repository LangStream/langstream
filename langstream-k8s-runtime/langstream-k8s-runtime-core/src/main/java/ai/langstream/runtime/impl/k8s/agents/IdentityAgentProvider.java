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
import ai.langstream.impl.agents.AbstractComposableAgentProvider;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Set;

import static ai.langstream.runtime.impl.k8s.KubernetesClusterRuntime.CLUSTER_TYPE;

/**
 * Implements support for the identity function.
 */
@Slf4j
public class IdentityAgentProvider extends AbstractComposableAgentProvider {

    private static final Set<String> SUPPORTED_AGENT_TYPES = Set.of("identity");

    public IdentityAgentProvider() {
        super(SUPPORTED_AGENT_TYPES, List.of(CLUSTER_TYPE, "none"));
    }

    @Override
    protected ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        return ComponentType.PROCESSOR;
    }
}
