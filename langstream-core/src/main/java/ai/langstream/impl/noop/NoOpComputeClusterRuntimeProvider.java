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
package ai.langstream.impl.noop;

import ai.langstream.api.runtime.ComputeClusterRuntime;
import ai.langstream.api.runtime.ComputeClusterRuntimeProvider;
import ai.langstream.api.runtime.DeployContext;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.ExecutionPlanOptimiser;
import ai.langstream.api.runtime.StreamingClusterRuntime;
import ai.langstream.impl.agents.ComposableAgentExecutionPlanOptimiser;
import ai.langstream.impl.common.BasicClusterRuntime;
import java.util.List;
import java.util.Map;

/** This is a dummy implementation of a ClusterRuntimeProvider useful mostly for unit tests. */
public class NoOpComputeClusterRuntimeProvider implements ComputeClusterRuntimeProvider {

    public static final String CLUSTER_TYPE = "none";

    @Override
    public boolean supports(String type) {
        return "none".equals(type);
    }

    @Override
    public ComputeClusterRuntime getImplementation() {
        return new NoOpClusterRuntime();
    }

    public static class NoOpClusterRuntime extends BasicClusterRuntime {

        static final List<ExecutionPlanOptimiser> OPTIMISERS =
                List.of(new ComposableAgentExecutionPlanOptimiser());

        @Override
        public String getClusterType() {
            return CLUSTER_TYPE;
        }

        @Override
        public void initialize(Map<String, Object> configuration) {}

        @Override
        public List<ExecutionPlanOptimiser> getExecutionPlanOptimisers() {
            return OPTIMISERS;
        }

        @Override
        public Object deploy(
                String tenant,
                ExecutionPlan applicationInstance,
                StreamingClusterRuntime streamingClusterRuntime,
                String codeStorageArchiveId,
                DeployContext deployContext) {
            return null;
        }

        @Override
        public void delete(
                String tenant,
                ExecutionPlan applicationInstance,
                StreamingClusterRuntime streamingClusterRuntime,
                String codeStorageArchiveId,
                DeployContext deployContext) {}
    }
}
