/**
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
package com.datastax.oss.sga.impl.deploy;

import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.runtime.ComputeClusterRuntime;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.PluginsRegistry;
import com.datastax.oss.sga.api.runtime.ClusterRuntimeRegistry;
import com.datastax.oss.sga.api.runtime.StreamingClusterRuntime;
import com.datastax.oss.sga.impl.common.ApplicationPlaceholderResolver;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Builder
@Slf4j
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
          log.info("Building execution plan for application {}", applicationInstance);
          final Application resolvedApplicationInstance = ApplicationPlaceholderResolver
                  .resolvePlaceholders(applicationInstance);
          log.info("After resolving the placeholders {}", resolvedApplicationInstance);
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

    /**
     * In the tests we don't have the operator, but we want to clean up the resources.
     * @param tenant
     * @param physicalApplicationInstance
     */
    public void deleteStreamingClusterResourcesForTests(String tenant, ExecutionPlan physicalApplicationInstance) {
        Application applicationInstance = physicalApplicationInstance.getApplication();
        StreamingClusterRuntime streamingClusterRuntime = registry.getStreamingClusterRuntime(applicationInstance.getInstance().streamingCluster());
        streamingClusterRuntime.delete(physicalApplicationInstance);
    }

    @Override
    public void close() {
        registry.close();
    }
}
