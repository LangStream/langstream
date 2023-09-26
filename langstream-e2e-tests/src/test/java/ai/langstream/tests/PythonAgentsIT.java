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
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
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
        deployLocalApplicationAndAwaitReady(
                tenant, applicationId, appDir, Map.of("SECRET1_VK", "super secret value"), 1);
        executeCommandOnClient(
                "bin/langstream gateway produce %s produce-input -v my-value --connect-timeout 30 -p sessionId=s1"
                        .formatted(applicationId)
                        .split(" "));

        String output =
                executeCommandOnClient(
                        ("bin/langstream gateway consume %s consume-output --position earliest -n 1 --connect-timeout "
                                        + "30 -p sessionId=s1")
                                .formatted(applicationId)
                                .split(" "));
        log.info("Output: {}", output);
        Assertions.assertTrue(
                output.contains(
                        "{\"record\":{\"key\":null,\"value\":\"my-value!!super secret value\","
                                + "\"headers\":{\"langstream-client-session-id\":\"s1\"}}"));

        updateLocalApplicationAndAwaitReady(
                tenant,
                applicationId,
                appDir,
                Map.of("SECRET1_VK", "super secret value - changed"),
                1);

        executeCommandOnClient(
                "bin/langstream gateway produce %s produce-input -v my-value --connect-timeout 30 -p sessionId=s2"
                        .formatted(applicationId)
                        .split(" "));

        output =
                executeCommandOnClient(
                        ("bin/langstream gateway consume %s consume-output --position earliest -n 1 --connect-timeout "
                                        + "30 -p sessionId=s2")
                                .formatted(applicationId)
                                .split(" "));
        log.info("Output2: {}", output);
        Assertions.assertTrue(
                output.contains(
                        "{\"record\":{\"key\":null,\"value\":\"my-value!!super secret value - changed\","
                                + "\"headers\":{\"langstream-client-session-id\":\"s2\"}}"));

        deleteAppAndAwaitCleanup(tenant, applicationId);

        final List<String> topics = getAllTopics();
        Assertions.assertEquals(List.of("ls-test-topic0"), topics);
    }

    @Test
    public void testSource() {
        installLangStreamCluster(true);
        final String tenant = "ten-" + System.currentTimeMillis();
        setupTenant(tenant);
        final String applicationId = "my-test-app";
        deployLocalApplicationAndAwaitReady(
                tenant, applicationId, "experimental-python-source", Map.of(), 1);
        final String output =
                executeCommandOnClient(
                        "bin/langstream gateway consume %s consume-output --position earliest -n 1 --connect-timeout 30"
                                .formatted(applicationId)
                                .split(" "));
        log.info("Output: {}", output);
        Assertions.assertTrue(
                output.contains("{\"record\":{\"key\":null,\"value\":\"test\",\"headers\":{}}"));

        deleteAppAndAwaitCleanup(tenant, applicationId);
    }

    @Test
    public void testSink() {
        installLangStreamCluster(true);
        final String tenant = "ten-" + System.currentTimeMillis();
        setupTenant(tenant);
        final String applicationId = "my-test-app";
        Map<String, Object> admin =
                (Map<String, Object>)
                        streamingCluster.configuration().getOrDefault("admin", Map.of());
        String bootStrapServers =
                (String) admin.getOrDefault("bootstrap.servers", "localhost:9092");
        deployLocalApplicationAndAwaitReady(
                tenant,
                applicationId,
                "experimental-python-sink",
                Map.of("KAFKA_BOOTSTRAP_SERVERS", bootStrapServers),
                1);

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
                        "{\"record\":{\"key\":null,\"value\":\"write: my-value\",\"headers\":{}}"));

        deleteAppAndAwaitCleanup(tenant, applicationId);
    }
}
