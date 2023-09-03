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

import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@Slf4j
@ExtendWith(BaseEndToEndTest.class)
public class PythonFunctionIT extends BaseEndToEndTest {

    @Test
    public void test() {
        installLangStreamCluster(false);
        final String tenant = "ten-" + System.currentTimeMillis();
        executeCommandOnClient(
                """
                bin/langstream tenants put %s &&
                bin/langstream configure tenant %s"""
                        .formatted(tenant, tenant)
                        .replace(System.lineSeparator(), " ")
                        .split(" "));
        String testAppsBaseDir = "src/test/resources/apps";
        String testInstanceBaseDir = "src/test/resources/instances";
        String testSecretBaseDir = "src/test/resources/secrets";
        final String applicationId = "my-test-app";
        copyFileToClientContainer(
                Paths.get(testAppsBaseDir, "python-processor").toFile(), "/tmp/python-processor");
        copyFileToClientContainer(
                Paths.get(testInstanceBaseDir, "kafka-kubernetes.yaml").toFile(),
                "/tmp/instance.yaml");
        copyFileToClientContainer(
                Paths.get(testSecretBaseDir, "secret1.yaml").toFile(), "/tmp/secrets.yaml");

        executeCommandOnClient(
                "bin/langstream apps deploy %s -app /tmp/python-processor -i /tmp/instance.yaml -s /tmp/secrets.yaml"
                        .formatted(applicationId)
                        .split(" "));
        client.apps()
                .statefulSets()
                .inNamespace(TENANT_NAMESPACE_PREFIX + tenant)
                .withName(applicationId + "-test-python-processor")
                .waitUntilReady(4, TimeUnit.MINUTES);

        executeCommandOnClient(
                "bin/langstream gateway produce %s produce-input -v my-value --connect-timeout 30"
                        .formatted(applicationId)
                        .split(" "));

        final String output =
                executeCommandOnClient(
                        "bin/langstream gateway consume %s consume-output --position earliest -n 1 --connect-timeout 30"
                                .formatted(applicationId)
                                .split(" "));
        log.info("Output: {}", output);
        Assertions.assertTrue(
                output.contains(
                        "{\"record\":{\"key\":null,\"value\":\"my-value!!super secret value\","
                                + "\"headers\":{}}"));
    }
}
