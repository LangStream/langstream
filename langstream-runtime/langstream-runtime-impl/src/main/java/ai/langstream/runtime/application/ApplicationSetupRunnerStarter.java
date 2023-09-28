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

import ai.langstream.api.model.Secrets;
import ai.langstream.runtime.RuntimeStarter;
import ai.langstream.runtime.agent.AgentCodeDownloader;
import ai.langstream.runtime.api.ClusterConfiguration;
import ai.langstream.runtime.api.agent.AgentCodeDownloaderConstants;
import ai.langstream.runtime.api.agent.DownloadAgentCodeConfiguration;
import ai.langstream.runtime.api.application.ApplicationSetupConfiguration;
import ai.langstream.runtime.api.application.ApplicationSetupConstants;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ApplicationSetupRunnerStarter extends RuntimeStarter {

    private static final ObjectMapper MAPPER =
            new ObjectMapper()
                    .configure(
                            FAIL_ON_UNKNOWN_PROPERTIES,
                            false); // this helps with forward compatibility

    private static final ErrorHandler errorHandler =
            error -> {
                log.error("Unexpected error", error);
                System.exit(-1);
            };

    public interface ErrorHandler {
        void handleError(Throwable error);
    }

    public static void main(String... args) {
        try {
            new ApplicationSetupRunnerStarter(new ApplicationSetupRunner()).start(args);
            // exit as soon as possible, we don't want to wait for the daemon threads
            System.exit(0);
        } catch (Throwable error) {
            errorHandler.handleError(error);
        }
    }

    private final ApplicationSetupRunner applicationSetupRunner;

    public ApplicationSetupRunnerStarter(ApplicationSetupRunner applicationSetupRunner) {
        this.applicationSetupRunner = applicationSetupRunner;
    }

    @Override
    public void start(String... args) throws Exception {
        if (args.length == 0) {
            throw new IllegalArgumentException("Missing arguments");
        }
        final Path clusterRuntimeConfigPath =
                getPathFromEnv(
                        ApplicationSetupConstants.CLUSTER_RUNTIME_CONFIG_ENV,
                        ApplicationSetupConstants.CLUSTER_RUNTIME_CONFIG_ENV_DEFAULT);
        final Path appConfigPath =
                getPathFromEnv(
                        ApplicationSetupConstants.APP_CONFIG_ENV,
                        ApplicationSetupConstants.APP_CONFIG_ENV_DEFAULT);
        final Path secretsPath = getOptionalPathFromEnv(ApplicationSetupConstants.APP_SECRETS_ENV);
        final Path packagesDirectory =
                getPathFromEnv(
                        ApplicationSetupConstants.AGENTS_ENV,
                        ApplicationSetupConstants.AGENTS_ENV_DEFAULT);

        final Map<String, Map<String, Object>> clusterRuntimeConfiguration =
                loadClusterRuntimeConfiguration(clusterRuntimeConfigPath);
        final ApplicationSetupConfiguration configuration =
                loadApplicationSetupConfiguration(appConfigPath);
        final Secrets secrets = loadSecrets(secretsPath);
        final Path codeDirectory = downloadApplicationCode(configuration);

        final String arg0 = args[0];
        switch (arg0) {
            case "deploy" -> applicationSetupRunner.runSetup(
                    clusterRuntimeConfiguration,
                    configuration,
                    secrets,
                    packagesDirectory,
                    codeDirectory);
            case "cleanup" -> applicationSetupRunner.runCleanup(
                    clusterRuntimeConfiguration,
                    configuration,
                    secrets,
                    packagesDirectory,
                    codeDirectory);
            default -> throw new IllegalArgumentException("Unknown command " + arg0);
        }
    }

    @SneakyThrows
    private Path downloadApplicationCode(
            ApplicationSetupConfiguration applicationSetupConfiguration) {
        if (applicationSetupConfiguration.getCodeArchiveId() == null) {
            log.warn(
                    "No code archive id provided, skipping download of application code. This might be an old version of the deployer/runtime.");
            return null;
        }

        final Path clusterConfigPath =
                getOptionalPathFromEnv(
                        AgentCodeDownloaderConstants.CLUSTER_CONFIG_ENV,
                        AgentCodeDownloaderConstants.CLUSTER_CONFIG_ENV_DEFAULT);
        if (clusterConfigPath == null) {
            log.warn(
                    "No cluster config provided, skipping download of application code. This might be an old version of the deployer/runtime.");
            return null;
        }
        final Path tokenPath =
                getOptionalPathFromEnv(
                        AgentCodeDownloaderConstants.TOKEN_ENV,
                        AgentCodeDownloaderConstants.TOKEN_ENV_DEFAULT);
        final String token;
        if (tokenPath != null) {
            token = Files.readString(tokenPath);
        } else {
            token = null;
        }

        final Path downloadDirectory = Files.createTempDirectory("langstream-code");

        ClusterConfiguration clusterConfiguration =
                MAPPER.readValue(clusterConfigPath.toFile(), ClusterConfiguration.class);

        AgentCodeDownloader downloader = new AgentCodeDownloader();

        final DownloadAgentCodeConfiguration downloadConfig =
                new DownloadAgentCodeConfiguration(
                        downloadDirectory.toFile().getAbsolutePath(),
                        applicationSetupConfiguration.getTenant(),
                        applicationSetupConfiguration.getApplicationId(),
                        applicationSetupConfiguration.getCodeArchiveId());

        log.info(
                "Downloading application code {} with cluster config {}",
                downloadConfig,
                clusterConfiguration);
        downloader.downloadCustomCode(clusterConfiguration, token, downloadConfig);
        log.info("Downloaded application code to {}", downloadDirectory);
        return downloadDirectory;
    }

    private Secrets loadSecrets(Path secretsPath) throws IOException {
        Secrets secrets = null;
        if (secretsPath != null) {
            log.info("Loading secrets from {}", secretsPath);
            secrets = MAPPER.readValue(secretsPath.toFile(), Secrets.class);
        } else {
            log.info("No secrets file provided");
        }
        return secrets;
    }

    private ApplicationSetupConfiguration loadApplicationSetupConfiguration(Path appConfigPath)
            throws IOException {
        log.info("Loading app configuration from {}", appConfigPath);
        final ApplicationSetupConfiguration configuration =
                MAPPER.readValue(appConfigPath.toFile(), ApplicationSetupConfiguration.class);
        log.info("applicationSetupConfiguration {}", configuration);
        return configuration;
    }

    private Map<String, Map<String, Object>> loadClusterRuntimeConfiguration(
            Path clusterRuntimeConfigPath) throws IOException {
        log.info("Loading cluster runtime config from {}", clusterRuntimeConfigPath);
        final Map<String, Map<String, Object>> clusterRuntimeConfiguration =
                (Map<String, Map<String, Object>>)
                        MAPPER.readValue(clusterRuntimeConfigPath.toFile(), Map.class);
        log.info("clusterRuntimeConfiguration {}", clusterRuntimeConfiguration);
        return clusterRuntimeConfiguration;
    }
}
