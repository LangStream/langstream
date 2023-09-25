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

import ai.langstream.deployer.k8s.api.crds.agents.AgentCustomResource;
import ai.langstream.deployer.k8s.api.crds.apps.ApplicationCustomResource;
import ai.langstream.tests.util.BaseEndToEndTest;
import io.fabric8.kubernetes.api.model.Secret;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Slf4j
@ExtendWith(BaseEndToEndTest.class)
public class PythonAgentsIT extends BaseEndToEndTest {

    @ParameterizedTest
    @ValueSource(strings = {"python-processor", "experimental-python-processor"})
    public void testProcessor(String appDir) throws Exception {
        installLangStreamCluster(true);
        final String tenant = "ten-" + System.currentTimeMillis();
        setupTenant(tenant);
        final String applicationId = "my-test-app";
        deployLocalApplication(applicationId, appDir, Map.of("SECRET1_VK", "super secret value"));
        final String tenantNamespace = TENANT_NAMESPACE_PREFIX + tenant;
        awaitApplicationReady(applicationId, 1);

        executeCommandOnClient(
                "bin/langstream gateway produce %s produce-input -v my-value --connect-timeout 30 -p sessionId=s1"
                        .formatted(applicationId)
                        .split(" "));

        String output =
                executeCommandOnClient(
                        "bin/langstream gateway consume %s consume-output --position earliest -n 1 --connect-timeout 30 -p sessionId=s1"
                                .formatted(applicationId)
                                .split(" "));
        log.info("Output: {}", output);
        Assertions.assertTrue(
                output.contains(
                        "{\"record\":{\"key\":null,\"value\":\"my-value!!super secret value\","
                                + "\"headers\":{\"langstream-client-session-id\":\"s1\"}}"));

        updateLocalApplication(
                applicationId, appDir, Map.of("SECRET1_VK", "super secret value - changed"));
        Thread.sleep(5000);
        awaitApplicationReady(applicationId, 1);

        executeCommandOnClient(
                "bin/langstream gateway produce %s produce-input -v my-value --connect-timeout 30 -p sessionId=s2"
                        .formatted(applicationId)
                        .split(" "));

        output =
                executeCommandOnClient(
                        "bin/langstream gateway consume %s consume-output --position earliest -n 1 --connect-timeout 30 -p sessionId=s2"
                                .formatted(applicationId)
                                .split(" "));
        log.info("Output2: {}", output);
        Assertions.assertTrue(
                output.contains(
                        "{\"record\":{\"key\":null,\"value\":\"my-value!!super secret value - changed\","
                                + "\"headers\":{\"langstream-client-session-id\":\"s2\"}}"));

        executeCommandOnClient("bin/langstream apps delete %s".formatted(applicationId).split(" "));

        awaitCleanup(tenantNamespace, applicationId, "-test-python-processor");

        final List<String> topics = getAllTopics();
        Assertions.assertEquals(List.of("ls-test-topic0"), topics);
    }

    @Test
    public void testSource() {
        installLangStreamCluster(true);
        final String tenant = "ten-" + System.currentTimeMillis();
        setupTenant(tenant);
        final String applicationId = "my-test-app";
        deployLocalApplication(applicationId, "experimental-python-source");
        final String tenantNamespace = TENANT_NAMESPACE_PREFIX + tenant;
        awaitApplicationReady(applicationId, 1);

        final String output =
                executeCommandOnClient(
                        "bin/langstream gateway consume %s consume-output --position earliest -n 1 --connect-timeout 30"
                                .formatted(applicationId)
                                .split(" "));
        log.info("Output: {}", output);
        Assertions.assertTrue(
                output.contains("{\"record\":{\"key\":null,\"value\":\"test\",\"headers\":{}}"));

        executeCommandOnClient("bin/langstream apps delete %s".formatted(applicationId).split(" "));

        awaitCleanup(tenantNamespace, applicationId, "-test-python-source");
    }

    private static void awaitCleanup(
            String tenantNamespace, String applicationId, String appNameSuffix) {
        Awaitility.await()
                .atMost(1, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            Assertions.assertNull(
                                    client.apps()
                                            .statefulSets()
                                            .inNamespace(tenantNamespace)
                                            .withName(applicationId + appNameSuffix)
                                            .get());

                            Assertions.assertEquals(
                                    0,
                                    client.resources(AgentCustomResource.class)
                                            .inNamespace(tenantNamespace)
                                            .list()
                                            .getItems()
                                            .size());

                            Assertions.assertEquals(
                                    0,
                                    client.resources(ApplicationCustomResource.class)
                                            .inNamespace(tenantNamespace)
                                            .list()
                                            .getItems()
                                            .size());

                            Assertions.assertEquals(
                                    1,
                                    client.resources(Secret.class)
                                            .inNamespace(tenantNamespace)
                                            .list()
                                            .getItems()
                                            .size());
                        });
    }
}
