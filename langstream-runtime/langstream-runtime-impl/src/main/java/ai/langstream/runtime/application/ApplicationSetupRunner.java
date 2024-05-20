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
package ai.langstream.runtime.application;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

import ai.langstream.api.model.Application;
import ai.langstream.api.model.Secrets;
import ai.langstream.api.runner.assets.AssetManagerRegistry;
import ai.langstream.api.runner.topics.TopicConnectionsRuntimeRegistry;
import ai.langstream.api.runtime.ClusterRuntimeRegistry;
import ai.langstream.api.runtime.DeployContext;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.impl.deploy.ApplicationDeployer;
import ai.langstream.impl.nar.NarFileHandler;
import ai.langstream.runtime.agent.AgentRunner;
import ai.langstream.runtime.api.application.ApplicationSetupConfiguration;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URL;
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
            Path packagesDirectory,
            Path codeDirectory)
            throws Exception {

        final String applicationId = configuration.getApplicationId();
        log.info("Setup application {}", applicationId);
        final Application appInstance = parseApplicationInstance(configuration, secrets);

        try (NarFileHandler narFileHandler = getNarFileHandler(packagesDirectory, codeDirectory)) {
            narFileHandler.scan();
            try (ApplicationDeployer deployer =
                    buildDeployer(clusterRuntimeConfiguration, narFileHandler)) {
                final ExecutionPlan executionPlan =
                        deployer.createImplementation(applicationId, appInstance);
                deployer.setup(configuration.getTenant(), executionPlan);
                log.info("Application {} setup done", applicationId);
            }
        }
    }

    public void runCleanup(
            Map<String, Map<String, Object>> clusterRuntimeConfiguration,
            ApplicationSetupConfiguration configuration,
            Secrets secrets,
            Path packagesDirectory,
            Path codeDirectory)
            throws Exception {

        final String applicationId = configuration.getApplicationId();
        log.info("Cleanup application {}", applicationId);
        final Application appInstance = parseApplicationInstance(configuration, secrets);

        try (NarFileHandler narFileHandler = getNarFileHandler(packagesDirectory, codeDirectory)) {
            narFileHandler.scan();
            try (ApplicationDeployer deployer =
                    buildDeployer(clusterRuntimeConfiguration, narFileHandler)) {
                final ExecutionPlan executionPlan =
                        deployer.createImplementation(applicationId, appInstance);

                deployer.cleanup(configuration.getTenant(), executionPlan);
                log.info("Application {} cleanup done", applicationId);
            }
        }
    }

    private NarFileHandler getNarFileHandler(Path packagesDirectory, Path codeDirectory)
            throws Exception {
        List<URL> customLib = AgentRunner.buildCustomLibClasspath(codeDirectory);
        return new NarFileHandler(
                packagesDirectory, customLib, Thread.currentThread().getContextClassLoader());
    }

    private Application parseApplicationInstance(
            ApplicationSetupConfiguration configuration, Secrets secrets)
            throws JsonProcessingException {
        final String applicationConfig = configuration.getApplication();

        final Application appInstance = MAPPER.readValue(applicationConfig, Application.class);
        appInstance.setSecrets(secrets);
        return appInstance;
    }

    private ApplicationDeployer buildDeployer(
            Map<String, Map<String, Object>> clusterRuntimeConfiguration,
            NarFileHandler narFileHandler) {
        final TopicConnectionsRuntimeRegistry topicConnectionsRuntimeRegistry =
                getTopicConnectionsRuntimeRegistry(narFileHandler);

        AssetManagerRegistry assetManagerRegistry = new AssetManagerRegistry();
        assetManagerRegistry.setAssetManagerPackageLoader(narFileHandler);
        return ApplicationDeployer.builder()
                .registry(new ClusterRuntimeRegistry(clusterRuntimeConfiguration))
                .pluginsRegistry(new PluginsRegistry())
                .topicConnectionsRuntimeRegistry(topicConnectionsRuntimeRegistry)
                .assetManagerRegistry(assetManagerRegistry)
                .deployContext(DeployContext.NO_DEPLOY_CONTEXT)
                .build();
    }

    private TopicConnectionsRuntimeRegistry getTopicConnectionsRuntimeRegistry(
            NarFileHandler narFileHandler) {
        final TopicConnectionsRuntimeRegistry topicConnectionsRuntimeRegistry =
                new TopicConnectionsRuntimeRegistry();
        topicConnectionsRuntimeRegistry.setPackageLoader(narFileHandler);
        return topicConnectionsRuntimeRegistry;
    }
}
