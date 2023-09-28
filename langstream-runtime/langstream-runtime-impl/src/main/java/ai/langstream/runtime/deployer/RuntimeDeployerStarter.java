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

import ai.langstream.api.model.Secrets;
import ai.langstream.runtime.RuntimeStarter;
import ai.langstream.runtime.api.ClusterConfiguration;
import ai.langstream.runtime.api.agent.AgentCodeDownloaderConstants;
import ai.langstream.runtime.api.deployer.RuntimeDeployerConfiguration;
import ai.langstream.runtime.api.deployer.RuntimeDeployerConstants;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/** This is the main entry point for the deployer runtime. */
@Slf4j
public class RuntimeDeployerStarter extends RuntimeStarter {

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
            new RuntimeDeployerStarter(new RuntimeDeployer()).start(args);
            // exit as soon as possible, we don't want to wait for the daemon threads
        } catch (Throwable error) {
            errorHandler.handleError(error);
        }
    }

    private final RuntimeDeployer runtimeDeployer;

    public RuntimeDeployerStarter(RuntimeDeployer runtimeDeployer) {
        this.runtimeDeployer = runtimeDeployer;
    }

    @Override
    public void start(String... args) throws Exception {
        // backward compatibility: <cmd> <clusterRuntimeConfigPath> <appConfigPath> <secretsPath>
        Path clusterRuntimeConfigPath;
        Path appConfigPath;
        Path secretsPath;
        if (args.length == 0) {
            throw new IllegalArgumentException("Missing arguments");
        }
        if (args.length == 1) {
            clusterRuntimeConfigPath =
                    getPathFromEnv(
                            RuntimeDeployerConstants.CLUSTER_RUNTIME_CONFIG_ENV,
                            RuntimeDeployerConstants.CLUSTER_RUNTIME_CONFIG_ENV_DEFAULT);
            appConfigPath =
                    getPathFromEnv(
                            RuntimeDeployerConstants.APP_CONFIG_ENV,
                            RuntimeDeployerConstants.APP_CONFIG_ENV_DEFAULT);
            secretsPath = getOptionalPathFromEnv(RuntimeDeployerConstants.APP_SECRETS_ENV);
        } else if (args.length == 2) {
            clusterRuntimeConfigPath = Path.of(args[1]);
            appConfigPath =
                    getPathFromEnv(
                            RuntimeDeployerConstants.APP_CONFIG_ENV,
                            RuntimeDeployerConstants.APP_CONFIG_ENV_DEFAULT);
            secretsPath = getOptionalPathFromEnv(RuntimeDeployerConstants.APP_SECRETS_ENV);
        } else if (args.length == 3) {
            clusterRuntimeConfigPath = Path.of(args[1]);
            appConfigPath = Path.of(args[2]);
            secretsPath = getOptionalPathFromEnv(RuntimeDeployerConstants.APP_SECRETS_ENV);
        } else if (args.length == 4) {
            clusterRuntimeConfigPath = Path.of(args[1]);
            appConfigPath = Path.of(args[2]);
            secretsPath = Path.of(args[3]);
        } else {
            throw new IllegalArgumentException("Invalid arguments: " + String.join(", ", args));
        }
        final String arg0 = args[0];

        log.info("Loading cluster runtime config from {}", clusterRuntimeConfigPath);
        final Map<String, Map<String, Object>> clusterRuntimeConfiguration =
                (Map<String, Map<String, Object>>)
                        MAPPER.readValue(clusterRuntimeConfigPath.toFile(), Map.class);
        log.info("clusterRuntimeConfiguration {}", clusterRuntimeConfiguration);

        log.info("Loading app configuration from {}", appConfigPath);
        final RuntimeDeployerConfiguration configuration =
                MAPPER.readValue(appConfigPath.toFile(), RuntimeDeployerConfiguration.class);
        log.info("RuntimeDeployerConfiguration {}", configuration);

        Secrets secrets = null;
        if (secretsPath != null) {
            log.info("Loading secrets from {}", secretsPath);
            secrets = MAPPER.readValue(secretsPath.toFile(), Secrets.class);
        } else {
            log.info("No secrets file provided");
        }
        final ClusterConfiguration clusterConfiguration = getClusterConfiguration();
        final String token = getToken();

        switch (arg0) {
            case "delete" -> runtimeDeployer.delete(
                    clusterRuntimeConfiguration, configuration, secrets);
            case "deploy" -> runtimeDeployer.deploy(
                    clusterRuntimeConfiguration,
                    configuration,
                    secrets,
                    clusterConfiguration,
                    token);
            default -> throw new IllegalArgumentException("Unknown command " + arg0);
        }
    }

    private ClusterConfiguration getClusterConfiguration() throws IOException {
        final Path clusterConfigPath =
                getOptionalPathFromEnv(
                        RuntimeDeployerConstants.CLUSTER_CONFIG_ENV,
                        RuntimeDeployerConstants.CLUSTER_CONFIG_ENV_DEFAULT);
        final ClusterConfiguration clusterConfiguration;
        if (clusterConfigPath == null || !Files.exists(clusterConfigPath)) {
            clusterConfiguration = null;
        } else {
            clusterConfiguration =
                    MAPPER.readValue(clusterConfigPath.toFile(), ClusterConfiguration.class);
        }
        return clusterConfiguration;
    }

    private String getToken() throws IOException {
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
        return token;
    }
}
