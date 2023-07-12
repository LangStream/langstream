package com.datastax.oss.sga.cluster.runtime;

import com.datastax.oss.sga.api.model.ApplicationInstance;
import com.datastax.oss.sga.api.runtime.ClusterRuntimeRegistry;
import com.datastax.oss.sga.api.runtime.PhysicalApplicationInstance;
import com.datastax.oss.sga.api.runtime.PluginsRegistry;
import com.datastax.oss.sga.impl.deploy.ApplicationDeployer;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import lombok.extern.slf4j.Slf4j;

/**
 * This is the main entry point for the cluster runtime job.
 */
@Slf4j
public class ClusterRuntimeRunner {

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
//            if (args.length < 1) {
//                throw new IllegalArgumentException("Missing pod configuration file argument");
//            }
            Path podRuntimeConfiguration = Path.of(args[0]);

            final ClusterRuntimeRunnerConfiguration configuration =
                    MAPPER.readValue(podRuntimeConfiguration.toFile(), ClusterRuntimeRunnerConfiguration.class);

            final String applicationName = configuration.getName();
            final String applicationConfig = configuration.getApplication();

            final ApplicationInstance appInstance =
                    MAPPER.readValue(applicationConfig, ApplicationInstance.class);

            ApplicationDeployer deployer = ApplicationDeployer
                    .builder()
                    .registry(new ClusterRuntimeRegistry())
                    .pluginsRegistry(new PluginsRegistry())
                    .build();

            final PhysicalApplicationInstance implementation = deployer.createImplementation(appInstance);
            deployer.deploy(implementation);
        } catch (Throwable error) {
            errorHandler.handleError(error);
            return;
        }
    }
}
