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
import io.fabric8.kubernetes.api.model.Secret;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Slf4j
@ExtendWith(BaseEndToEndTest.class)
public class PythonFunctionIT extends BaseEndToEndTest {

    @ParameterizedTest
    @ValueSource(strings = {"python-processor", "experimental-python-processor"})
    public void test(String appDir) {
        installLangStreamCluster(false);
        final String tenant = "ten-" + System.currentTimeMillis();
        setupTenant(tenant);
        final String applicationId = "my-test-app";
        deployLocalApplication(applicationId, appDir);
        final String tenantNamespace = TENANT_NAMESPACE_PREFIX + tenant;
        awaitApplicationReady(applicationId, 1);

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

        executeCommandOnClient("bin/langstream apps delete %s".formatted(applicationId).split(" "));

        Awaitility.await()
                .atMost(1, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            Assertions.assertNull(
                                    client.apps()
                                            .statefulSets()
                                            .inNamespace(tenantNamespace)
                                            .withName(applicationId + "-test-python-processor")
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

        final List<String> topics = getAllTopicsFromKafka();
        Assertions.assertEquals(List.of("TEST_TOPIC_0"), topics);
    }
}
