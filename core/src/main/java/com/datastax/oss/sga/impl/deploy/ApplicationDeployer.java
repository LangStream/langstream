package com.datastax.oss.sga.impl.deploy;

import com.datastax.oss.sga.api.model.ApplicationInstance;
import com.datastax.oss.sga.api.runtime.ClusterRuntime;
import com.datastax.oss.sga.api.runtime.PhysicalApplicationInstance;
import com.datastax.oss.sga.api.runtime.PluginsRegistry;
import com.datastax.oss.sga.api.runtime.ClusterRuntimeRegistry;
import com.datastax.oss.sga.api.runtime.StreamingClusterRuntime;
import com.datastax.oss.sga.impl.common.ApplicationInstancePlaceholderResolver;
import com.datastax.oss.sga.impl.storage.ApplicationStore;
import lombok.Builder;

@Builder
public final class ApplicationDeployer {

      private ClusterRuntimeRegistry registry;
      private PluginsRegistry pluginsRegistry;
      private ApplicationStore applicationStore;

    /**
     * Create a new implementation of the application instance.
     * @param applicationInstance
     * @return the new application
     */
      public PhysicalApplicationInstance createImplementation(ApplicationInstance applicationInstance) {
          ClusterRuntime clusterRuntime = registry.getClusterRuntime(applicationInstance.getInstance().computeCluster());
          StreamingClusterRuntime streamingClusterRuntime = registry.getStreamingClusterRuntime(applicationInstance.getInstance().streamingCluster());
          final ApplicationInstance resolvedApplicationInstance = ApplicationInstancePlaceholderResolver
                  .resolvePlaceholders(applicationInstance);
          return clusterRuntime.createImplementation(resolvedApplicationInstance, pluginsRegistry, streamingClusterRuntime);
      }

    /**
     * Deploy the application instance.
     * @param physicalApplicationInstance
     */
    public void deploy(PhysicalApplicationInstance physicalApplicationInstance) {
      ApplicationInstance applicationInstance = physicalApplicationInstance.getApplicationInstance();
      ClusterRuntime clusterRuntime = registry.getClusterRuntime(applicationInstance.getInstance().computeCluster());
      StreamingClusterRuntime streamingClusterRuntime = registry.getStreamingClusterRuntime(applicationInstance.getInstance().streamingCluster());
      final ApplicationInstance resolvedApplicationInstance = ApplicationInstancePlaceholderResolver
              .resolvePlaceholders(applicationInstance);
      clusterRuntime.deploy(resolvedApplicationInstance, physicalApplicationInstance, streamingClusterRuntime);
  }

    /**
     * Delete the application instance and all the resources associated with it.
     * @param physicalApplicationInstance
     */
    public void delete(PhysicalApplicationInstance physicalApplicationInstance) {
      ApplicationInstance applicationInstance = physicalApplicationInstance.getApplicationInstance();
      ClusterRuntime clusterRuntime = registry.getClusterRuntime(applicationInstance.getInstance().computeCluster());
      StreamingClusterRuntime streamingClusterRuntime = registry.getStreamingClusterRuntime(applicationInstance.getInstance().streamingCluster());
      clusterRuntime.delete(applicationInstance, physicalApplicationInstance, streamingClusterRuntime);
  }
}
