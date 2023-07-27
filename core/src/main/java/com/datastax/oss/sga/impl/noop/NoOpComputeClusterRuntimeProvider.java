package com.datastax.oss.sga.impl.noop;

import com.datastax.oss.sga.api.runtime.ComputeClusterRuntime;
import com.datastax.oss.sga.api.runtime.ComputeClusterRuntimeProvider;
import com.datastax.oss.sga.api.runtime.ExecutionPlanOptimiser;
import com.datastax.oss.sga.impl.agents.AbstractCompositeAgentProvider;
import com.datastax.oss.sga.impl.agents.ComposableAgentExecutionPlanOptimiser;
import com.datastax.oss.sga.impl.agents.ai.GenAIToolKitExecutionPlanOptimizer;
import com.datastax.oss.sga.impl.common.BasicClusterRuntime;

import java.util.List;
import java.util.Map;

/**
 * This is a dummy implementation of a ClusterRuntimeProvider useful mostly for unit tests.
 */
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

        static final List<ExecutionPlanOptimiser> OPTIMISERS = List.of(
                new GenAIToolKitExecutionPlanOptimizer(),
                new ComposableAgentExecutionPlanOptimiser());

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
    }

}
