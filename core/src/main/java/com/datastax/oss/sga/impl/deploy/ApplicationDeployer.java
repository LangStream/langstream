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
public final class ApplicationDeployer implements AutoCloseable {

      private ClusterRuntimeRegistry registry;
      private PluginsRegistry pluginsRegistry;

    /**
     * Create a new implementation of the application instance.
     * @param applicationInstance
     * @return the new application
     */
      public ExecutionPlan createImplementation(String applicationId, Application applicationInstance) {
          ComputeClusterRuntime clusterRuntime = registry.getClusterRuntime(applicationInstance.getInstance().computeCluster());
          StreamingClusterRuntime streamingClusterRuntime = registry.getStreamingClusterRuntime(applicationInstance.getInstance().streamingCluster());
          final Application resolvedApplicationInstance = ApplicationPlaceholderResolver
                  .resolvePlaceholders(applicationInstance);
          return clusterRuntime.buildExecutionPlan(applicationId, resolvedApplicationInstance, pluginsRegistry, streamingClusterRuntime);
      }

    /**
     * Deploy the application instance.
     *
     * @param physicalApplicationInstance
     * @param codeStorageArchiveId
     */
    public Object deploy(String tenant, ExecutionPlan physicalApplicationInstance, String codeStorageArchiveId) {
      Application applicationInstance = physicalApplicationInstance.getApplication();
      ComputeClusterRuntime clusterRuntime = registry.getClusterRuntime(applicationInstance.getInstance().computeCluster());
      StreamingClusterRuntime streamingClusterRuntime = registry.getStreamingClusterRuntime(applicationInstance.getInstance().streamingCluster());
      return clusterRuntime.deploy(tenant, physicalApplicationInstance, streamingClusterRuntime, codeStorageArchiveId);
  }

    /**
     * Delete the application instance and all the resources associated with it.
     *
     * @param physicalApplicationInstance
     * @param codeStorageArchiveId
     */
    public void delete(String tenant, ExecutionPlan physicalApplicationInstance, String codeStorageArchiveId) {
      Application applicationInstance = physicalApplicationInstance.getApplication();
      ComputeClusterRuntime clusterRuntime = registry.getClusterRuntime(applicationInstance.getInstance().computeCluster());
      StreamingClusterRuntime streamingClusterRuntime = registry.getStreamingClusterRuntime(applicationInstance.getInstance().streamingCluster());
      clusterRuntime.delete(tenant, physicalApplicationInstance, streamingClusterRuntime, codeStorageArchiveId);
  }

    @Override
    public void close() {
        registry.close();
    }
}
