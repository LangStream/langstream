package com.datastax.oss.sga.impl.dummy;

import com.datastax.oss.sga.api.runtime.ClusterRuntime;
import com.datastax.oss.sga.api.runtime.ClusterRuntimeProvider;
import com.datastax.oss.sga.impl.common.BasicClusterRuntime;

/**
 * This is a dummy implementation of a ClusterRuntimeProvider useful mostly for unit tests.
 */
public class NoOpClusterRuntimeProvider implements ClusterRuntimeProvider {
    @Override
    public boolean supports(String type) {
        return "none".equals(type);
    }

    @Override
    public ClusterRuntime getImplementation() {
        return new NoOpClusterRuntime();
    }

    public static class NoOpClusterRuntime extends BasicClusterRuntime {
        @Override
        public String getClusterType() {
            return "none";
        }
    }
}
