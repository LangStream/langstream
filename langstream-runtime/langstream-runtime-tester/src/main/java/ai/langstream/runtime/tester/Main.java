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
package ai.langstream.runtime.tester;

import ai.langstream.api.model.Application;
import ai.langstream.api.storage.GlobalMetadataStore;
import ai.langstream.api.storage.GlobalMetadataStoreRegistry;
import ai.langstream.api.webservice.application.ApplicationDescription;
import ai.langstream.api.webservice.tenant.TenantConfiguration;
import ai.langstream.apigateway.LangStreamApiGateway;
import ai.langstream.impl.common.ApplicationPlaceholderResolver;
import ai.langstream.impl.parser.ModelBuilder;
import ai.langstream.impl.storage.GlobalMetadataStoreManager;
import ai.langstream.runtime.agent.api.AgentAPIController;
import ai.langstream.runtime.agent.api.MetricsHttpServlet;
import ai.langstream.webservice.LangStreamControlPlaneWebApplication;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.prometheus.client.hotspot.DefaultExports;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

@Slf4j
public class Main {
    public static void main(String... args) {
        try {

            String tenant = System.getenv().getOrDefault("LANSGSTREAM_TESTER_TENANT", "tenant");
            String applicationId =
                    System.getenv().getOrDefault("LANSGSTREAM_TESTER_APPLICATIONID", "app");

            String singleAgentId = System.getenv().getOrDefault("LANSGSTREAM_TESTER_AGENTID", "");

            boolean startWebservices =
                    Boolean.parseBoolean(
                            System.getenv()
                                    .getOrDefault("LANSGSTREAM_TESTER_STARTWEBSERVICES", "true"));

            boolean dryRunMode =
                    Boolean.parseBoolean(
                            System.getenv().getOrDefault("LANSGSTREAM_TESTER_DRYRUN", "false"));
            String applicationPath = "/code/application";
            String instanceFile = "/code/instance.yaml";
            String secretsFile = "/code/secrets.yaml";
            String agentsDirectory = "/app/agents";

            final String secrets;
            final Path secretsPath = Paths.get(secretsFile);
            if (Files.exists(secretsPath)) {
                secrets = Files.readString(secretsPath);
            } else {
                secrets = null;
            }
            String instance = Files.readString(Paths.get(instanceFile));

            Path codeDirectory = Paths.get(applicationPath);
            List<Path> applicationDirectories = List.of(codeDirectory);
            ModelBuilder.ApplicationWithPackageInfo applicationWithPackageInfo =
                    ModelBuilder.buildApplicationInstance(
                            applicationDirectories, instance, secrets);

            if (dryRunMode) {
                log.info("Dry run mode");
                final Application resolved =
                        ApplicationPlaceholderResolver.resolvePlaceholders(
                                applicationWithPackageInfo.getApplication());
                final ApplicationDescription.ApplicationDefinition def =
                        new ApplicationDescription.ApplicationDefinition(resolved);

                final String asString = yamlPrinter().writeValueAsString(def);
                log.info("Application:\n{}", asString);
                return;
            }

            final Path basePersistentStatePath = Paths.get("/app/persistent-state");
            Files.createDirectories(basePersistentStatePath);

            List<String> expectedAgents = new ArrayList<>();
            List<String> allAgentIds = new ArrayList<>();
            applicationWithPackageInfo
                    .getApplication()
                    .getModules()
                    .values()
                    .forEach(
                            module -> {
                                module.getPipelines()
                                        .values()
                                        .forEach(
                                                p -> {
                                                    p.getAgents()
                                                            .forEach(
                                                                    agentConfiguration -> {
                                                                        allAgentIds.add(
                                                                                agentConfiguration
                                                                                        .getId());
                                                                        expectedAgents.add(
                                                                                applicationId
                                                                                        + "-"
                                                                                        + agentConfiguration
                                                                                                .getId());
                                                                    });
                                                });
                            });
            log.info("Available Agent ids in this application  {}", expectedAgents);

            List<String> agentsToRun = new ArrayList<>(expectedAgents);
            List<String> agentsIdToKeepInStats = new ArrayList<>(allAgentIds);
            if (!singleAgentId.isEmpty()) {
                log.info("Filtering out all the agents but {}", singleAgentId);
                if (!allAgentIds.contains(singleAgentId)) {
                    throw new IllegalStateException(
                            "Agent id "
                                    + singleAgentId
                                    + " not found in the list of available agents for this application ("
                                    + allAgentIds
                                    + ")");
                }
                agentsToRun.clear();
                agentsToRun.add(applicationId + "-" + singleAgentId);

                agentsIdToKeepInStats.clear();
                agentsIdToKeepInStats.add(singleAgentId);
            }

            Server server = null;
            try (LocalApplicationRunner runner =
                    new LocalApplicationRunner(
                            Paths.get(agentsDirectory), codeDirectory, basePersistentStatePath); ) {

                server = bootstrapHttpServer(runner);
                server.start();
                InMemoryApplicationStore.setAgentsInfoCollector(runner);
                InMemoryApplicationStore.setFilterAgents(agentsIdToKeepInStats);

                if (startWebservices) {
                    LangStreamControlPlaneWebApplication.main(
                            "--spring.config.location=classpath:webservice.application.properties");

                    LangStreamApiGateway.main(
                            "--spring.config.location=classpath:gateway.application.properties");
                }

                final GlobalMetadataStore globalMetadataStore =
                        GlobalMetadataStoreRegistry.loadStore("local", Map.of());
                final GlobalMetadataStoreManager globalMetadataStoreManager =
                        new GlobalMetadataStoreManager(
                                globalMetadataStore, new InMemoryApplicationStore());
                globalMetadataStoreManager.putTenant(tenant, TenantConfiguration.builder().build());
                runner.start();
                try (LocalApplicationRunner.ApplicationRuntime applicationRuntime =
                        runner.deployApplicationWithSecrets(
                                tenant,
                                applicationId,
                                applicationWithPackageInfo,
                                expectedAgents.toArray(new String[0]))) {

                    Runtime.getRuntime()
                            .addShutdownHook(
                                    new Thread(
                                            () -> {
                                                log.info("Shutdown hook");
                                                runner.close();
                                            }));

                    runner.executeAgentRunners(applicationRuntime, agentsToRun);
                }
            } finally {
                if (server != null) {
                    server.stop();
                }
            }

        } catch (Throwable error) {
            error.printStackTrace();
            System.exit(1);
        }
    }

    private static ObjectMapper yamlPrinter() {
        return new ObjectMapper(new YAMLFactory())
                .enable(SerializationFeature.INDENT_OUTPUT)
                .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
    }

    private static Server bootstrapHttpServer(LocalApplicationRunner runner) throws Exception {
        DefaultExports.initialize();
        Server server = new Server(8790);
        log.info("Started local agent controller service at port 8790");
        String url = "http://" + InetAddress.getLocalHost().getCanonicalHostName() + ":8790";
        log.info("The addresses should be {}/metrics and {}/info}", url, url);
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");
        server.setHandler(context);
        context.addServlet(new ServletHolder(new MetricsHttpServlet()), "/metrics");
        context.addServlet(new ServletHolder(new AgentControllerServlet(runner)), "/commands/*");
        server.start();
        return server;
    }

    private static class AgentControllerServlet extends HttpServlet {
        private static final ObjectMapper MAPPER = new ObjectMapper();

        private final LocalApplicationRunner agentAPIController;

        public AgentControllerServlet(LocalApplicationRunner runner) {
            this.agentAPIController = runner;
        }

        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            resp.setContentType("application/json");
            log.info("Received request {}", req.getRequestURI());
            String uri = req.getRequestURI();
            if (uri.endsWith("/restart")) {
                try {
                    Collection<AgentAPIController> agents =
                            agentAPIController.collectAgentsStatus().values();
                    List<Map<String, Object>> result = new ArrayList<>();
                    for (AgentAPIController controller : agents) {
                        result.add(controller.restart());
                    }
                    MAPPER.writeValue(resp.getOutputStream(), result);
                } catch (Throwable error) {
                    log.error("Error while restarting the agents");
                    resp.getOutputStream().write((error + "").getBytes());
                    resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                }
            } else {
                resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
            }
        }
    }
}
