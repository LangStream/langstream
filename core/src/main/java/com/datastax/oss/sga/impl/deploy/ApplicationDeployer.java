package com.datastax.oss.sga.impl.deploy;

import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.runtime.ComputeClusterRuntime;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.PluginsRegistry;
import com.datastax.oss.sga.api.runtime.ClusterRuntimeRegistry;
import com.datastax.oss.sga.api.runtime.StreamingClusterRuntime;
import com.datastax.oss.sga.impl.common.ApplicationPlaceholderResolver;
import lombok.Builder;

@Builder
public final class ApplicationDeployer {

      private ClusterRuntimeRegistry registry;
      private PluginsRegistry pluginsRegistry;

    /**
     * Create a new implementation of the application instance.
     * @param applicationInstance
     * @return the new application
     */
      public ExecutionPlan createImplementation(Application applicationInstance) {
          ComputeClusterRuntime clusterRuntime = registry.getClusterRuntime(applicationInstance.getInstance().computeCluster());
          StreamingClusterRuntime streamingClusterRuntime = registry.getStreamingClusterRuntime(applicationInstance.getInstance().streamingCluster());
          final Application resolvedApplicationInstance = ApplicationPlaceholderResolver
                  .resolvePlaceholders(applicationInstance);
          return clusterRuntime.buildExecutionPlan(resolvedApplicationInstance, pluginsRegistry, streamingClusterRuntime);
      }

    /**
     * Deploy the application instance.
     * @param physicalApplicationInstance
     */
    public void deploy(ExecutionPlan physicalApplicationInstance) {
      Application applicationInstance = physicalApplicationInstance.getApplication();
      ComputeClusterRuntime clusterRuntime = registry.getClusterRuntime(applicationInstance.getInstance().computeCluster());
      StreamingClusterRuntime streamingClusterRuntime = registry.getStreamingClusterRuntime(applicationInstance.getInstance().streamingCluster());
      clusterRuntime.deploy(physicalApplicationInstance, streamingClusterRuntime);
  }

    /**
     * Delete the application instance and all the resources associated with it.
     * @param physicalApplicationInstance
     */
    public void delete(ExecutionPlan physicalApplicationInstance) {
      Application applicationInstance = physicalApplicationInstance.getApplication();
      ComputeClusterRuntime clusterRuntime = registry.getClusterRuntime(applicationInstance.getInstance().computeCluster());
      StreamingClusterRuntime streamingClusterRuntime = registry.getStreamingClusterRuntime(applicationInstance.getInstance().streamingCluster());
      clusterRuntime.delete(physicalApplicationInstance, streamingClusterRuntime);
  }
}
