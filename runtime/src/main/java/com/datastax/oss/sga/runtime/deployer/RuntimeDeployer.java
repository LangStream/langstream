package com.datastax.oss.sga.runtime.deployer;

import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.Secrets;
import com.datastax.oss.sga.api.runtime.ClusterRuntimeRegistry;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.PluginsRegistry;
import com.datastax.oss.sga.impl.deploy.ApplicationDeployer;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import lombok.extern.slf4j.Slf4j;

/**
 * This is the main entry point for the deployer runtime.
 */
@Slf4j
public class RuntimeDeployer {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static ErrorHandler errorHandler = error -> {
        log.error("Unexpected error", error);
        System.exit(-1);
    };

    public interface ErrorHandler {
        void handleError(Throwable error);
    }

    public static void main(String... args) {
        try {
            if (args.length < 1) {
                throw new IllegalArgumentException("Missing runtime deployer configuration");
            }
            Path configPath = Path.of(args[0]);
            Secrets secrets = null;
            if (args.length > 1) {
                Path secretsPath = Path.of(args[1]);
                log.info("Loading secrets from {}", secretsPath);
                secrets = MAPPER.readValue(secretsPath.toFile(), Secrets.class);

            }
            log.info("Loading configuration from {}", configPath);
            final RuntimeDeployerConfiguration configuration =
                    MAPPER.readValue(configPath.toFile(), RuntimeDeployerConfiguration.class);

            final String applicationName = configuration.getName();
            final String applicationConfig = configuration.getApplication();

            final Application appInstance =
                    MAPPER.readValue(applicationConfig, Application.class);
            appInstance.setSecrets(secrets);

            ApplicationDeployer deployer = ApplicationDeployer
                    .builder()
                    .registry(new ClusterRuntimeRegistry())
                    .pluginsRegistry(new PluginsRegistry())
                    .build();

            log.info("Deploying application {}", applicationName);
            final ExecutionPlan implementation = deployer.createImplementation(appInstance);
            deployer.deploy(implementation);
            log.info("Application {} deployed", applicationName);
        } catch (Throwable error) {
            errorHandler.handleError(error);
            return;
        }
    }
}

