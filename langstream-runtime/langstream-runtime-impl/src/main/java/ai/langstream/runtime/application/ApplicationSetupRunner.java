package ai.langstream.runtime.application;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import ai.langstream.api.model.Application;
import ai.langstream.api.model.AssetDefinition;
import ai.langstream.api.model.Secrets;
import ai.langstream.api.runner.assets.AssetManager;
import ai.langstream.api.runner.assets.AssetManagerAndLoader;
import ai.langstream.api.runner.assets.AssetManagerRegistry;
import ai.langstream.api.runner.topics.TopicConnectionsRuntime;
import ai.langstream.api.runner.topics.TopicConnectionsRuntimeRegistry;
import ai.langstream.api.runtime.AssetNode;
import ai.langstream.api.runtime.ClusterRuntimeRegistry;
import ai.langstream.api.runtime.DeployContext;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.api.webservice.application.ApplicationCodeInfo;
import ai.langstream.impl.deploy.ApplicationDeployer;
import ai.langstream.runtime.agent.nar.NarFileHandler;
import ai.langstream.runtime.api.application.ApplicationSetupConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ApplicationSetupRunner {

    private static final ObjectMapper MAPPER =
            new ObjectMapper()
                    .configure(
                            FAIL_ON_UNKNOWN_PROPERTIES,
                            false); // this helps with forward compatibility

    public void runSetup(
            Map<String, Map<String, Object>> clusterRuntimeConfiguration,
            ApplicationSetupConfiguration configuration,
            Secrets secrets,
            Path packagesDirectory)
            throws Exception {

        final String applicationId = configuration.getApplicationId();
        log.info("Setup application {}",applicationId);
        final String applicationConfig = configuration.getApplication();

        final Application appInstance = MAPPER.readValue(applicationConfig, Application.class);
        appInstance.setSecrets(secrets);

        try (NarFileHandler narFileHandler =
                     new NarFileHandler(
                             packagesDirectory,
                             List.of(),
                             Thread.currentThread().getContextClassLoader())) {
            narFileHandler.scan();
            final TopicConnectionsRuntimeRegistry topicConnectionsRuntimeRegistry =
                    getTopicConnectionsRuntimeRegistry(narFileHandler);

            try (ApplicationDeployer deployer =
                         buildDeployer(clusterRuntimeConfiguration, topicConnectionsRuntimeRegistry)) {
                final ExecutionPlan executionPlan =
                        deployer.createImplementation(applicationId, appInstance);

                deployTopics(topicConnectionsRuntimeRegistry, executionPlan);
                log.info("Application {} topics ready", applicationId);

                deployAssets(narFileHandler, executionPlan);
                log.info("Application {} assets ready", applicationId);
            }
        }
    }

    private void deployAssets(NarFileHandler narFileHandler, ExecutionPlan executionPlan) throws Exception {
        AssetManagerRegistry assetManagerRegistry = new AssetManagerRegistry();
        assetManagerRegistry.setAssetManagerPackageLoader(narFileHandler);

        for (AssetNode assetNode : executionPlan.getAssets()) {
            AssetDefinition asset = MAPPER.convertValue(assetNode.config(), AssetDefinition.class);
            deployAsset(asset, assetManagerRegistry);
        }
    }

    private void deployTopics(TopicConnectionsRuntimeRegistry topicConnectionsRuntimeRegistry,
                           ExecutionPlan executionPlan) {
        TopicConnectionsRuntime topicConnectionsRuntime =
                topicConnectionsRuntimeRegistry
                        .getTopicConnectionsRuntime(
                                executionPlan
                                        .getApplication()
                                        .getInstance()
                                        .streamingCluster())
                        .asTopicConnectionsRuntime();
        topicConnectionsRuntime.deploy(executionPlan);
    }

    private ApplicationDeployer buildDeployer(Map<String, Map<String, Object>> clusterRuntimeConfiguration,
                                         TopicConnectionsRuntimeRegistry topicConnectionsRuntimeRegistry) {
        return ApplicationDeployer.builder()
                .registry(new ClusterRuntimeRegistry(clusterRuntimeConfiguration))
                .pluginsRegistry(new PluginsRegistry())
                .topicConnectionsRuntimeRegistry(topicConnectionsRuntimeRegistry)
                .build();
    }

    private TopicConnectionsRuntimeRegistry getTopicConnectionsRuntimeRegistry(NarFileHandler narFileHandler) {
        final TopicConnectionsRuntimeRegistry topicConnectionsRuntimeRegistry =
                new TopicConnectionsRuntimeRegistry();
        topicConnectionsRuntimeRegistry.setPackageLoader(narFileHandler);
        return topicConnectionsRuntimeRegistry;
    }


    private void deployAsset(AssetDefinition asset, AssetManagerRegistry assetManagerRegistry)
            throws Exception {
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
                                "Asset {}  of type {} needs to be created",
                                asset.getName(),
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
}
