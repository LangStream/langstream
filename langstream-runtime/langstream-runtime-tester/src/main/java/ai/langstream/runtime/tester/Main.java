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

import ai.langstream.apigateway.LangStreamApiGateway;
import ai.langstream.impl.parser.ModelBuilder;
import ai.langstream.webservice.LangStreamControlPlaneWebApplication;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

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

            String applicationPath = "/code/application";
            String instanceFile = "/code/instance.yaml";
            String secretsFile = "/code/secrets.yaml";
            String agentsDirectory = "/app/agents";

            String secrets = Files.readString(Paths.get(secretsFile));
            String instance = Files.readString(Paths.get(instanceFile));

            Path codeDirectory = Paths.get(applicationPath);
            List<Path> applicationDirectories = List.of(codeDirectory);
            ModelBuilder.ApplicationWithPackageInfo applicationWithPackageInfo =
                    ModelBuilder.buildApplicationInstance(
                            applicationDirectories, instance, secrets);

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

            try (LocalApplicationRunner runner =
                    new LocalApplicationRunner(Paths.get(agentsDirectory), codeDirectory); ) {

                InMemoryApplicationStore.setAgentsInfoCollector(runner);
                InMemoryApplicationStore.setFilterAgents(agentsIdToKeepInStats);

                if (startWebservices) {
                    LangStreamControlPlaneWebApplication.main(
                            "--spring.config.location=classpath:webservice.application.properties");

                    LangStreamApiGateway.main(
                            "--spring.config.location=classpath:gateway.application.properties");
                }

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
            }

        } catch (Throwable error) {
            error.printStackTrace();
        }
    }
}
