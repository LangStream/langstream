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
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@Slf4j
@ExtendWith(BaseEndToEndTest.class)
@Tag(TestSuites.CATEGORY_PYTHON)
public class PythonAgentsIT extends BaseEndToEndTest {

    @Test
    public void testProcessor() {
        installLangStreamCluster(true);
        final String tenant = "ten-" + System.currentTimeMillis();
        setupTenant(tenant);
        final String applicationId = "my-test-app";
        deployLocalApplicationAndAwaitReady(
                tenant,
                applicationId,
                "python-processor",
                Map.of("SECRET1_VK", "super secret value"),
                1);
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

        output =
                executeCommandOnClient(
                        ("bin/langstream gateway consume %s topic-producer --position earliest -n 1 --connect-timeout 30")
                                .formatted(applicationId)
                                .split(" "));
        log.info("Output2: {}", output);
        Assertions.assertTrue(
                output.contains(
                        "{\"record\":{\"key\":null,\"value\":\"my-value test-topic-producer\",\"headers\":{}}"));

        updateLocalApplicationAndAwaitReady(
                tenant,
                applicationId,
                "python-processor",
                Map.of("SECRET1_VK", "super secret value - changed"),
                1);

        // test force-restart
        updateLocalApplicationAndAwaitReady(
                tenant,
                applicationId,
                "python-processor",
                Map.of("SECRET1_VK", "super secret value - changed"),
                1,
                true);

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
        log.info("Output3: {}", output);
        Assertions.assertTrue(
                output.contains(
                        "{\"record\":{\"key\":null,\"value\":\"my-value!!super secret value - changed\","
                                + "\"headers\":{\"langstream-client-session-id\":\"s2\"}}"));

        deleteAppAndAwaitCleanup(tenant, applicationId);

        final List<String> topics = getAllTopics();
        log.info("all topics: {}", topics);
        Assertions.assertTrue(topics.contains("ls-test-topic0"));
        Assertions.assertFalse(topics.contains("ls-test-topic1"));
        Assertions.assertTrue(topics.contains("ls-test-topic-producer"));
    }

    @Test
    public void testSource() {
        installLangStreamCluster(true);
        final String tenant = "ten-" + System.currentTimeMillis();
        setupTenant(tenant);
        final String applicationId = "my-test-app";
        deployLocalApplicationAndAwaitReady(tenant, applicationId, "python-source", Map.of(), 1);
        final String output =
                executeCommandOnClient(
                        "bin/langstream gateway consume %s consume-output --position earliest -n 1 --connect-timeout 30"
                                .formatted(applicationId)
                                .split(" "));
        log.info("Output: {}", output);
        String bigPayload = "test".repeat(10000);
        String value = "the length is " + bigPayload.length();
        Assertions.assertTrue(
                output.contains(
                        "{\"record\":{\"key\":null,\"value\":\"" + value + "\",\"headers\":{}}"),
                "Output doesn't contain the expected payload: " + output);

        deleteAppAndAwaitCleanup(tenant, applicationId);
    }

    @Test
    public void testSink() throws Exception {
        installLangStreamCluster(true);
        final String tenant = "ten-" + System.currentTimeMillis();
        setupTenant(tenant);
        final String applicationId = "my-test-app";
        Map<String, Object> admin =
                (Map<String, Object>)
                        streamingCluster.configuration().getOrDefault("admin", Map.of());
        final String pythonKafkaProducerConfig = JSON_MAPPER.writeValueAsString(admin);
        deployLocalApplicationAndAwaitReady(
                tenant,
                applicationId,
                "python-sink",
                Map.of("KAFKA_PRODUCER_CONFIG", pythonKafkaProducerConfig),
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
