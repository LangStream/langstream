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
package ai.langstream.tests;

import ai.langstream.tests.util.BaseEndToEndTest;
import ai.langstream.tests.util.TestSuites;
import java.io.File;
import java.nio.file.Files;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@Slf4j
@ExtendWith(BaseEndToEndTest.class)
@Tag(TestSuites.CATEGORY_OTHER)
public class AppLifecycleIT extends BaseEndToEndTest {

    @Test
    public void testDeleteBrokenApplication() throws Exception {
        installLangStreamCluster(true);
        final String tenant = "ten-" + System.currentTimeMillis();
        setupTenant(tenant);
        final String applicationId = "my-test-app";

        final Map<String, Map<String, Object>> instanceContent =
                Map.of(
                        "instance",
                        Map.of(
                                "streamingCluster",
                                Map.of(
                                        "type",
                                        "kafka",
                                        "configuration",
                                        Map.of("bootstrapServers", "wrong:9092")),
                                "computeCluster",
                                Map.of("type", "kubernetes")));

        final File instanceFile = Files.createTempFile("ls-test", ".yaml").toFile();
        YAML_MAPPER.writeValue(instanceFile, instanceContent);

        deployLocalApplication(
                tenant, false, applicationId, "python-processor", instanceFile, Map.of());
        awaitApplicationInStatus(applicationId, "ERROR_DEPLOYING");
        executeCommandOnClient(
                "bin/langstream apps delete %s ".formatted(applicationId).split(" "));
        awaitApplicationInStatus(applicationId, "ERROR_DELETING");
        executeCommandOnClient(
                "bin/langstream apps delete -f %s".formatted(applicationId).split(" "));
        awaitApplicationCleanup(tenant, applicationId);
    }

    @Test
    public void testUpdateForceRestart() throws Exception {
        installLangStreamCluster(true);
        final String tenant = "ten-" + System.currentTimeMillis();
        setupTenant(tenant);
        final String applicationId = "my-test-app";

        final Map<String, Map<String, Object>> instanceContent =
                Map.of(
                        "instance",
                        Map.of(
                                "streamingCluster",
                                Map.of(
                                        "type",
                                        "kafka",
                                        "configuration",
                                        Map.of("bootstrapServers", "wrong:9092")),
                                "computeCluster",
                                Map.of("type", "kubernetes")));

        final File instanceFile = Files.createTempFile("ls-test", ".yaml").toFile();
        YAML_MAPPER.writeValue(instanceFile, instanceContent);

        deployLocalApplication(
                tenant, false, applicationId, "python-processor", instanceFile, Map.of());
        awaitApplicationInStatus(applicationId, "ERROR_DEPLOYING");
        executeCommandOnClient(
                "bin/langstream apps delete %s ".formatted(applicationId).split(" "));
        awaitApplicationInStatus(applicationId, "ERROR_DELETING");
        executeCommandOnClient(
                "bin/langstream apps delete -f %s".formatted(applicationId).split(" "));
        awaitApplicationCleanup(tenant, applicationId);
    }
}
