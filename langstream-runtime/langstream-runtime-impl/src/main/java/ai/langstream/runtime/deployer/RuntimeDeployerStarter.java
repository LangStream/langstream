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
package ai.langstream.runtime.deployer;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import ai.langstream.api.model.Secrets;
import ai.langstream.runtime.api.deployer.RuntimeDeployerConfiguration;
import ai.langstream.runtime.api.deployer.RuntimeDeployerConstants;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * This is the main entry point for the deployer runtime.
 */
@Slf4j
public class RuntimeDeployerStarter {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(FAIL_ON_UNKNOWN_PROPERTIES, false); // this helps with forward compatibility
    private static final ErrorHandler errorHandler = error -> {
        log.error("Unexpected error", error);
        System.exit(-1);
    };

    public interface ErrorHandler {
        void handleError(Throwable error);
    }

    public static void main(String... args) {
        try {

            new RuntimeDeployerStarter()
                    .run(new RuntimeDeployer(), args);
        } catch (Throwable error) {
            errorHandler.handleError(error);
        }
    }

    public void run(RuntimeDeployer runtimeDeployer, String... args) throws Exception {
        // backward compatibility: <cmd> <clusterRuntimeConfigPath> <appConfigPath> <secretsPath>
        Path clusterRuntimeConfigPath;
        Path appConfigPath;
        Path secretsPath;
        if (args.length == 0) {
            throw new IllegalArgumentException("Missing arguments");
        }
        if (args.length == 1) {
            clusterRuntimeConfigPath =
                    getPathFromEnv(RuntimeDeployerConstants.CLUSTER_RUNTIME_CONFIG_ENV,
                            RuntimeDeployerConstants.CLUSTER_RUNTIME_CONFIG_ENV_DEFAULT);
            appConfigPath = getPathFromEnv(RuntimeDeployerConstants.APP_CONFIG_ENV,
                    RuntimeDeployerConstants.APP_CONFIG_ENV_DEFAULT);
            secretsPath = getOptionalPathFromEnv(RuntimeDeployerConstants.APP_SECRETS_ENV);
        } else if (args.length == 2) {
            clusterRuntimeConfigPath = Path.of(args[1]);
            appConfigPath = getPathFromEnv(RuntimeDeployerConstants.APP_CONFIG_ENV,
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
                (Map<String, Map<String, Object>>) MAPPER.readValue(clusterRuntimeConfigPath.toFile(), Map.class);
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

        switch (arg0) {
            case "delete" -> runtimeDeployer.delete(clusterRuntimeConfiguration, configuration, secrets);
            case "deploy" -> runtimeDeployer.deploy(clusterRuntimeConfiguration, configuration, secrets);
            default -> throw new IllegalArgumentException("Unknown command " + arg0);
        }

    }

    private Path getPathFromEnv(String envVar, String defaultValue) {
        return getPathFromEnv(envVar, defaultValue, true);
    }

    private Path getOptionalPathFromEnv(String envVar) {
        return getPathFromEnv(envVar, null, false);
    }

    private Path getPathFromEnv(String envVar, String defaultValue, boolean required) {
        String value = getEnv(envVar);
        if (value == null) {
            value = defaultValue;
        }
        if (!required && value == null) {
            return null;
        }
        final Path path = Path.of(value);
        if (!path.toFile().exists()) {
            throw new IllegalArgumentException("File " + path + " does not exist");
        }
        return path;
    }

    String getEnv(String key) {
        return System.getenv(key);
    }
}

