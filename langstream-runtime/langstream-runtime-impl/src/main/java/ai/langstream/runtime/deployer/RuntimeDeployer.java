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
package ai.langstream.runtime.deployer;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

import ai.langstream.admin.client.AdminClient;
import ai.langstream.admin.client.AdminClientLogger;
import ai.langstream.api.model.Application;
import ai.langstream.api.model.Secrets;
import ai.langstream.api.runtime.ClusterRuntimeRegistry;
import ai.langstream.api.runtime.DeployContext;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.api.webservice.application.ApplicationCodeInfo;
import ai.langstream.impl.deploy.ApplicationDeployer;
import ai.langstream.runtime.api.ClusterConfiguration;
import ai.langstream.runtime.api.deployer.RuntimeDeployerConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/** This is the main entry point for the deployer runtime. */
@Slf4j
public class RuntimeDeployer {

    private static final ObjectMapper MAPPER =
            new ObjectMapper()
                    .configure(
                            FAIL_ON_UNKNOWN_PROPERTIES,
                            false); // this helps with forward compatibility

    public void deploy(
            Map<String, Map<String, Object>> clusterRuntimeConfiguration,
            RuntimeDeployerConfiguration configuration,
            Secrets secrets,
            ClusterConfiguration clusterConfiguration,
            String token)
            throws Exception {

        final String applicationId = configuration.getApplicationId();
        log.info(
                "Deploying application {} codeStorageArchiveId {}",
                applicationId,
                configuration.getCodeStorageArchiveId());
        final String applicationConfig = configuration.getApplication();

        final Application appInstance = MAPPER.readValue(applicationConfig, Application.class);
        appInstance.setSecrets(secrets);

        final String tenant = configuration.getTenant();
        RuntimeDeployerConfiguration.DeployFlags deployFlags = configuration.getDeployFlags();
        if (deployFlags == null) {
            deployFlags = new RuntimeDeployerConfiguration.DeployFlags();
        }

        final DeployContextImpl deployContext =
                new DeployContextImpl(
                        clusterConfiguration,
                        token,
                        tenant,
                        deployFlags.isAutoUpgradeRuntimeImage(),
                        deployFlags.isAutoUpgradeRuntimeImagePullPolicy(),
                        deployFlags.isAutoUpgradeAgentResources(),
                        deployFlags.isAutoUpgradeAgentPodTemplate(),
                        deployFlags.getSeed());
        try (ApplicationDeployer deployer =
                ApplicationDeployer.builder()
                        .registry(new ClusterRuntimeRegistry(clusterRuntimeConfiguration))
                        .pluginsRegistry(new PluginsRegistry())
                        .deployContext(deployContext)
                        .build()) {

            final ExecutionPlan implementation =
                    deployer.createImplementation(applicationId, appInstance);

            deployer.deploy(tenant, implementation, configuration.getCodeStorageArchiveId());
            log.info("Application {} deployed", applicationId);
        }
    }

    public void delete(
            Map<String, Map<String, Object>> clusterRuntimeConfiguration,
            RuntimeDeployerConfiguration configuration,
            Secrets secrets)
            throws IOException {

        final String applicationId = configuration.getApplicationId();
        final String applicationConfig = configuration.getApplication();

        final Application appInstance = MAPPER.readValue(applicationConfig, Application.class);
        appInstance.setSecrets(secrets);

        try (ApplicationDeployer deployer =
                ApplicationDeployer.builder()
                        .registry(new ClusterRuntimeRegistry(clusterRuntimeConfiguration))
                        .pluginsRegistry(new PluginsRegistry())
                        .deployContext(DeployContext.NO_DEPLOY_CONTEXT)
                        .build()) {

            log.info("Deleting application {}", applicationId);
            final ExecutionPlan implementation =
                    deployer.createImplementation(applicationId, appInstance);
            deployer.delete(
                    configuration.getTenant(),
                    implementation,
                    configuration.getCodeStorageArchiveId());
            log.info("Application {} deleted", applicationId);
        }
    }

    private static class DeployContextImpl implements DeployContext {

        private final AdminClient adminClient;
        private final boolean autoUpgradeRuntimeImage;
        private final boolean autoUpgradeRuntimeImagePullPolicy;
        private final boolean autoUpgradeAgentResources;
        private final boolean autoUpgradeAgentPodTemplate;
        private final long seed;

        public DeployContextImpl(
                ClusterConfiguration clusterConfiguration,
                String token,
                String tenant,
                boolean autoUpgradeRuntimeImage,
                boolean autoUpgradeRuntimeImagePullPolicy,
                boolean autoUpgradeAgentResources,
                boolean autoUpgradeAgentPodTemplate,
                long seed) {
            if (clusterConfiguration == null) {
                adminClient = null;
            } else {
                adminClient = createAdminClient(clusterConfiguration, token, tenant);
            }
            this.autoUpgradeRuntimeImage = autoUpgradeRuntimeImage;
            this.autoUpgradeRuntimeImagePullPolicy = autoUpgradeRuntimeImagePullPolicy;
            this.autoUpgradeAgentResources = autoUpgradeAgentResources;
            this.autoUpgradeAgentPodTemplate = autoUpgradeAgentPodTemplate;
            this.seed = seed;
        }

        @Override
        public boolean isAutoUpgradeRuntimeImage() {
            return autoUpgradeRuntimeImage;
        }

        @Override
        public boolean isAutoUpgradeRuntimeImagePullPolicy() {
            return autoUpgradeRuntimeImagePullPolicy;
        }

        @Override
        public boolean isAutoUpgradeAgentResources() {
            return autoUpgradeAgentResources;
        }

        @Override
        public boolean isAutoUpgradeAgentPodTemplate() {
            return autoUpgradeAgentPodTemplate;
        }

        @Override
        public long getApplicationSeed() {
            return seed;
        }

        @Override
        @SneakyThrows
        public ApplicationCodeInfo getApplicationCodeInfo(
                String tenant, String applicationId, String codeArchiveId) {
            if (adminClient == null) {
                log.warn("Cluster configuration was not set, returning null application code info");
                return null;
            }
            final String codeInfo =
                    adminClient.applications().getCodeInfo(applicationId, codeArchiveId);
            return MAPPER.readValue(codeInfo, ApplicationCodeInfo.class);
        }

        @Override
        public void close() {
            if (adminClient != null) {
                adminClient.close();
            }
        }
    }

    private static AdminClient createAdminClient(
            ClusterConfiguration clusterConfiguration, String token, String tenant) {
        final ai.langstream.admin.client.AdminClientConfiguration config =
                ai.langstream.admin.client.AdminClientConfiguration.builder()
                        .webServiceUrl(clusterConfiguration.controlPlaneUrl())
                        .token(token)
                        .tenant(tenant)
                        .build();

        final AdminClientLogger logger =
                new AdminClientLogger() {
                    @Override
                    public void log(Object message) {
                        log.info(message.toString());
                    }

                    @Override
                    public void error(Object message) {
                        log.error(message.toString());
                    }

                    @Override
                    public boolean isDebugEnabled() {
                        return log.isDebugEnabled();
                    }

                    @Override
                    public void debug(Object message) {
                        log.debug(message.toString());
                    }
                };
        return new AdminClient(config, logger);
    }
}
