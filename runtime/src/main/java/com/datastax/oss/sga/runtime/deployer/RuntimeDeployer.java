package com.datastax.oss.sga.runtime.deployer;

import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.Secrets;
import com.datastax.oss.sga.api.runtime.ClusterRuntimeRegistry;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.PluginsRegistry;
import com.datastax.oss.sga.impl.deploy.ApplicationDeployer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
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
                throw new IllegalArgumentException("Missing runtime deployer command");
            }

            final String arg0 = args[0];

            if (args.length < 2) {
                throw new IllegalArgumentException("Missing runtime deployer configuration");
            }
            Path configPath = Path.of(args[1]);
            Secrets secrets = null;
            if (args.length > 1) {
                Path secretsPath = Path.of(args[2]);
                log.info("Loading secrets from {}", secretsPath);
                secrets = MAPPER.readValue(secretsPath.toFile(), Secrets.class);

            }
            log.info("Loading configuration from {}", configPath);
            final RuntimeDeployerConfiguration configuration =
                    MAPPER.readValue(configPath.toFile(), RuntimeDeployerConfiguration.class);

            switch (arg0) {
                case "delete":
                    delete(configuration, secrets);
                    break;
                case "deploy":
                    deploy(configuration, secrets);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown command " + arg0);
            }
        } catch (Throwable error) {
            errorHandler.handleError(error);
            return;
        }
    }

    private static void deploy(RuntimeDeployerConfiguration configuration, Secrets secrets) throws IOException {


        final String applicationName = configuration.getName();
        log.info("Deploying application {}", applicationName);
        final String applicationConfig = configuration.getApplication();

        final Application appInstance =
                MAPPER.readValue(applicationConfig, Application.class);
        appInstance.setSecrets(secrets);

        ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        final ExecutionPlan implementation = deployer.createImplementation(appInstance);
        deployer.deploy(implementation);
        log.info("Application {} deployed", applicationName);
    }

    private static void delete(RuntimeDeployerConfiguration configuration, Secrets secrets) throws IOException {


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

        log.info("Deleting application {}", applicationName);
        final ExecutionPlan implementation = deployer.createImplementation(appInstance);
        deployer.delete(implementation);
        log.info("Application {} deleted", applicationName);
    }
}

