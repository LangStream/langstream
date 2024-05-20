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
package ai.langstream.impl.deploy;

import ai.langstream.api.model.Application;
import ai.langstream.api.model.AssetDefinition;
import ai.langstream.api.runner.assets.AssetManager;
import ai.langstream.api.runner.assets.AssetManagerAndLoader;
import ai.langstream.api.runner.assets.AssetManagerRegistry;
import ai.langstream.api.runner.topics.TopicConnectionsRuntime;
import ai.langstream.api.runner.topics.TopicConnectionsRuntimeRegistry;
import ai.langstream.api.runtime.AssetNode;
import ai.langstream.api.runtime.ClusterRuntimeRegistry;
import ai.langstream.api.runtime.ComputeClusterRuntime;
import ai.langstream.api.runtime.DeployContext;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.api.runtime.StreamingClusterRuntime;
import ai.langstream.impl.common.ApplicationPlaceholderResolver;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Objects;
import lombok.Builder;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Builder
@Slf4j
public final class ApplicationDeployer implements AutoCloseable {

    static final ObjectMapper MAPPER = new ObjectMapper();

    private ClusterRuntimeRegistry registry;
    private PluginsRegistry pluginsRegistry;
    @Builder.Default private DeployContext deployContext = DeployContext.NO_DEPLOY_CONTEXT;
    @Getter private TopicConnectionsRuntimeRegistry topicConnectionsRuntimeRegistry;
    private AssetManagerRegistry assetManagerRegistry;

    /**
     * Create a new implementation of the application instance.
     *
     * @param applicationInstance the application instance
     * @return the new application
     */
    public ExecutionPlan createImplementation(
            String applicationId, Application applicationInstance) {
        ComputeClusterRuntime clusterRuntime =
                registry.getClusterRuntime(applicationInstance.getInstance().computeCluster());
        StreamingClusterRuntime streamingClusterRuntime =
                registry.getStreamingClusterRuntime(
                        applicationInstance.getInstance().streamingCluster());
        if (log.isDebugEnabled()) {
            log.debug("Building execution plan for application {}", applicationInstance);
        }
        final Application resolvedApplicationInstance =
                ApplicationPlaceholderResolver.resolvePlaceholders(applicationInstance);
        if (log.isDebugEnabled()) {
            log.debug("After resolving the placeholders {}", resolvedApplicationInstance);
        }
        return clusterRuntime.buildExecutionPlan(
                applicationId,
                resolvedApplicationInstance,
                pluginsRegistry,
                streamingClusterRuntime);
    }

    /**
     * Setup the application by deploying topics and assets.
     *
     * @param tenant
     * @param executionPlan
     */
    public void setup(String tenant, ExecutionPlan executionPlan) {
        setupTopics(executionPlan);
        setupAssets(executionPlan);
    }

    private void setupTopics(ExecutionPlan executionPlan) {
        TopicConnectionsRuntime topicConnectionsRuntime =
                topicConnectionsRuntimeRegistry
                        .getTopicConnectionsRuntime(
                                executionPlan.getApplication().getInstance().streamingCluster())
                        .asTopicConnectionsRuntime();
        topicConnectionsRuntime.deploy(executionPlan);
    }

    private void setupAssets(ExecutionPlan executionPlan) {
        for (AssetNode assetNode : executionPlan.getAssets()) {
            AssetDefinition asset = MAPPER.convertValue(assetNode.config(), AssetDefinition.class);
            setupAsset(asset, assetManagerRegistry);
        }
    }

    @SneakyThrows
    private void setupAsset(AssetDefinition asset, AssetManagerRegistry assetManagerRegistry) {
        log.info("Deploying asset {} type {}", asset.getId(), asset.getAssetType());
        AssetManagerAndLoader assetManager =
                assetManagerRegistry.getAssetManager(asset.getAssetType());
        if (assetManager == null) {
            throw new RuntimeException(
                    "No asset manager found for asset type " + asset.getAssetType());
        }
        try {
            String creationMode = asset.getCreationMode();
            switch (creationMode) {
                case AssetDefinition.CREATE_MODE_CREATE_IF_NOT_EXISTS -> {
                    AssetManager assetManagerImpl = assetManager.asAssetManager();
                    assetManagerImpl.initialize(asset);
                    boolean exists = assetManagerImpl.assetExists();

                    if (!exists) {
                        log.info(
                                "Asset {} of type {} needs to be created",
                                asset.getId(),
                                asset.getAssetType());
                        assetManagerImpl.deployAsset();
                    }
                }
                case AssetDefinition.CREATE_MODE_NONE -> {
                    return;
                }
            }
        } finally {
            assetManager.close();
        }
    }

    /**
     * Deploy the application instance.
     *
     * @param physicalApplicationInstance the application instance
     * @param codeStorageArchiveId the code storage archive id
     */
    public Object deploy(
            String tenant, ExecutionPlan physicalApplicationInstance, String codeStorageArchiveId) {
        Objects.requireNonNull(deployContext, "Deploy context is not set");
        Application applicationInstance = physicalApplicationInstance.getApplication();
        ComputeClusterRuntime clusterRuntime =
                registry.getClusterRuntime(applicationInstance.getInstance().computeCluster());
        StreamingClusterRuntime streamingClusterRuntime =
                registry.getStreamingClusterRuntime(
                        applicationInstance.getInstance().streamingCluster());
        return clusterRuntime.deploy(
                tenant,
                physicalApplicationInstance,
                streamingClusterRuntime,
                codeStorageArchiveId,
                deployContext);
    }

    /**
     * Undeploy the application and delete all the agents.
     *
     * @param tenant
     * @param executionPlan the application plan
     * @param codeStorageArchiveId the code storage archive id
     */
    public void delete(String tenant, ExecutionPlan executionPlan, String codeStorageArchiveId) {
        Objects.requireNonNull(deployContext, "Deploy context is not set");
        Application applicationInstance = executionPlan.getApplication();
        ComputeClusterRuntime clusterRuntime =
                registry.getClusterRuntime(applicationInstance.getInstance().computeCluster());
        StreamingClusterRuntime streamingClusterRuntime =
                registry.getStreamingClusterRuntime(
                        applicationInstance.getInstance().streamingCluster());
        clusterRuntime.delete(
                tenant,
                executionPlan,
                streamingClusterRuntime,
                codeStorageArchiveId,
                deployContext);
    }

    /**
     * Cleanup all the resources associated with an application.
     *
     * @param tenant
     * @param executionPlan the application instance
     */
    public void cleanup(String tenant, ExecutionPlan executionPlan) {
        cleanupTopics(executionPlan);
        cleanupAssets(executionPlan);
    }

    private void cleanupTopics(ExecutionPlan executionPlan) {
        TopicConnectionsRuntime topicConnectionsRuntime =
                topicConnectionsRuntimeRegistry
                        .getTopicConnectionsRuntime(
                                executionPlan.getApplication().getInstance().streamingCluster())
                        .asTopicConnectionsRuntime();
        topicConnectionsRuntime.delete(executionPlan);
    }

    private void cleanupAssets(ExecutionPlan executionPlan) {
        for (AssetNode assetNode : executionPlan.getAssets()) {
            AssetDefinition asset = MAPPER.convertValue(assetNode.config(), AssetDefinition.class);
            cleanupAsset(asset, assetManagerRegistry);
        }
    }

    @SneakyThrows
    private void cleanupAsset(AssetDefinition asset, AssetManagerRegistry assetManagerRegistry) {
        log.info(
                "Cleaning up asset {} type {} with deletion mode {}",
                asset.getId(),
                asset.getAssetType(),
                asset.getDeletionMode());
        AssetManagerAndLoader assetManager =
                assetManagerRegistry.getAssetManager(asset.getAssetType());
        if (assetManager == null) {
            throw new RuntimeException(
                    "No asset manager found for asset type " + asset.getAssetType());
        }
        try {
            String deletionMode = asset.getDeletionMode();
            switch (deletionMode) {
                case AssetDefinition.DELETE_MODE_DELETE:
                    {
                        AssetManager assetManagerImpl = assetManager.asAssetManager();
                        assetManagerImpl.initialize(asset);
                        log.info(
                                "Deleting asset {} of type {}",
                                asset.getId(),
                                asset.getAssetType());
                        assetManagerImpl.deleteAssetIfExists();
                        break;
                    }
                default:
                    {
                        log.info("Keep asset {} of type {}", asset.getId(), asset.getAssetType());
                    }
            }
        } finally {
            assetManager.close();
        }
    }

    @Override
    public void close() {
        registry.close();
        if (deployContext != null) {
            deployContext.close();
        }
    }
}
