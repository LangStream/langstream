package com.datastax.oss.sga.impl.noop;

import com.datastax.oss.sga.api.runtime.ComputeClusterRuntime;
import com.datastax.oss.sga.api.runtime.ComputeClusterRuntimeProvider;
import com.datastax.oss.sga.impl.common.BasicClusterRuntime;
import java.util.Map;

/**
 * This is a dummy implementation of a ClusterRuntimeProvider useful mostly for unit tests.
 */
public class NoOpComputeClusterRuntimeProvider implements ComputeClusterRuntimeProvider {
    @Override
    public boolean supports(String type) {
        return "none".equals(type);
    }

    @Override
    public ComputeClusterRuntime getImplementation() {
        return new NoOpClusterRuntime();
    }

    public static class NoOpClusterRuntime extends BasicClusterRuntime {
        @Override
        public String getClusterType() {
            return "none";
        }

        @Override
        public void initialize(Map<String, Object> configuration) {}
    }
}
