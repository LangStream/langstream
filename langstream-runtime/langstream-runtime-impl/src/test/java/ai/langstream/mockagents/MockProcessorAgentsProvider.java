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
package ai.langstream.mockagents;

import ai.langstream.api.model.AgentConfiguration;
import ai.langstream.api.runtime.ComponentType;
import ai.langstream.api.runtime.ComputeClusterRuntime;
import ai.langstream.impl.agents.AbstractComposableAgentProvider;
import ai.langstream.runtime.impl.k8s.KubernetesClusterRuntime;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MockProcessorAgentsProvider extends AbstractComposableAgentProvider {
    public MockProcessorAgentsProvider() {
        super(
                Set.of(
                        "mock-failing-processor",
                        "mock-failing-sink",
                        "mock-service",
                        "mock-async-processor",
                        "mock-stateful-processor"),
                List.of(KubernetesClusterRuntime.CLUSTER_TYPE));
    }

    @Override
    public boolean supports(String type, ComputeClusterRuntime clusterRuntime) {
        return super.supports(type, clusterRuntime);
    }

    @Override
    protected ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        switch (agentConfiguration.getType()) {
            case "mock-service":
                return ComponentType.SERVICE;
            default:
                return ComponentType.PROCESSOR;
        }
    }
}
