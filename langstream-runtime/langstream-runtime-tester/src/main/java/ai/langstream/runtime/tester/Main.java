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

import ai.langstream.impl.parser.ModelBuilder;
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
            String applicationPath = "/code/application";
            String instanceFile = "/code/instance.yaml";
            String secretsFile = "/code/secrets.yaml";
            String agentsDirectory = "/app/agents";

            String tenant = "tenant";

            String secrets = Files.readString(Paths.get(secretsFile));
            String instance = Files.readString(Paths.get(instanceFile));

            List<Path> applicationDirectories = List.of(Paths.get(applicationPath));
            ModelBuilder.ApplicationWithPackageInfo applicationWithPackageInfo =
                    ModelBuilder.buildApplicationInstance(
                            applicationDirectories, instance, secrets);

            String applicationId = "app";

            List<String> expectedAgents = new ArrayList<>();
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
                                                                        expectedAgents.add(
                                                                                applicationId
                                                                                        + "-"
                                                                                        + agentConfiguration
                                                                                                .getId());
                                                                    });
                                                });
                            });
            log.info("expectedAgents {}", expectedAgents);

            try (LocalApplicationRunner runner =
                    new LocalApplicationRunner(Paths.get(agentsDirectory)); ) {

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

                    runner.executeAgentRunners(applicationRuntime);
                }
            }
        } catch (Throwable error) {
            error.printStackTrace();
        }
    }
}
