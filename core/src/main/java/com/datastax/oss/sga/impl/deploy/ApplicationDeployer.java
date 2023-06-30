package com.datastax.oss.sga.impl.deploy;

import com.datastax.oss.sga.api.model.ApplicationInstance;
import com.datastax.oss.sga.api.runtime.ClusterRuntime;
import com.datastax.oss.sga.api.runtime.PhysicalApplicationInstance;
import com.datastax.oss.sga.impl.RuntimeRegistry;
import lombok.Builder;

@Builder
public class ApplicationDeployer<T extends PhysicalApplicationInstance> {

      private RuntimeRegistry registry;

      public T createImplementation(ApplicationInstance applicationInstance) {
          ClusterRuntime<T> clusterRuntime = registry.getClusterRuntime(applicationInstance.getInstance().streamingCluster());
          return (T) clusterRuntime.createImplementation(applicationInstance);
      }

      public void deploy(ApplicationInstance applicationInstance, T physicalApplicationInstance) {
          ClusterRuntime<T> clusterRuntime = registry.getClusterRuntime(applicationInstance.getInstance().streamingCluster());
          clusterRuntime.deploy(applicationInstance, physicalApplicationInstance);
      }

      public void delete(ApplicationInstance applicationInstance, T physicalApplicationInstance) {
          ClusterRuntime<T> clusterRuntime = registry.getClusterRuntime(applicationInstance.getInstance().streamingCluster());
          clusterRuntime.delete(applicationInstance, physicalApplicationInstance);
      }
}
