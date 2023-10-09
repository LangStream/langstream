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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import ai.langstream.tests.util.BaseEndToEndTest;
import ai.langstream.tests.util.TestSuites;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@Slf4j
@ExtendWith(BaseEndToEndTest.class)
@Tag(TestSuites.CATEGORY_OTHER)
public class CassandraSinkIT extends BaseEndToEndTest {

    @BeforeEach
    public void setupCassandra() {
        installCassandra();
    }

    @AfterEach
    public void removeCassandra() {
        uninstallCassandra();
    }

    @Test
    public void test() throws Exception {
        installLangStreamCluster(true);
        final String tenant = "ten-" + System.currentTimeMillis();
        setupTenant(tenant);
        final String applicationId = "my-test-app";
        String cassandraHost = "cassandra-0.cassandra." + namespace;
        final Map<String, String> env =
                Map.of(
                        "CASSANDRA_CONTACT_POINTS", cassandraHost,
                        "CASSANDRA_LOCAL_DC", "datacenter1",
                        "CASSANDRA_PORT", "9042");
        deployLocalApplicationAndAwaitReady(tenant, applicationId, "cassandra-sink", env, 1);

        executeCommandOnClient(
                "bin/langstream gateway produce %s produce-input -v '{\"id\": 10, \"name\": \"test-from-sink\", \"description\": \"test-from-sink\"}'"
                        .formatted(applicationId)
                        .split(" "));

        // in this timeout is also included the runtime pod startup time
        Awaitility.await()
                .atMost(1, TimeUnit.MINUTES)
                .pollInterval(5, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            try {
                                String contents = executeCQL("SELECT * FROM vsearch.products;");
                                assertTrue(contents.contains("test-from-sink"));
                            } catch (Throwable t) {
                                log.error("Failed to execute cqlsh command: {}", t.getMessage());
                                fail("Failed to execute cqlsh command: " + t.getMessage());
                            }
                        });

        executeCommandOnClient("bin/langstream apps delete %s".formatted(applicationId).split(" "));

        Awaitility.await()
                .atMost(1, TimeUnit.MINUTES)
                .pollInterval(5, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            try {
                                executeCQL("DESCRIBE KEYSPACE vsearch");
                                fail("Keyspace vsearch should not exist anymore");
                            } catch (Throwable t) {
                                log.info("Got exception: {}", t.getMessage());
                                assertTrue(
                                        t.getMessage()
                                                .contains("'vsearch' not found in keyspaces"));
                            }
                        });
    }

    private static final String CASSANDRA_MANIFEST =
            """
            apiVersion: v1
            kind: Service
            metadata:
              name: cassandra
              labels:
                app: cassandra
            spec:
              ports:
              - port: 9042
                name: cassandra
              clusterIP: None
              selector:
                app: cassandra
            ---
            apiVersion: apps/v1
            kind: StatefulSet
            metadata:
              name: cassandra
            spec:
              selector:
                matchLabels:
                  app: cassandra
              serviceName: "cassandra"
              replicas: 1
              minReadySeconds: 10 # by default is 0
              template:
                metadata:
                  labels:
                    app: cassandra
                spec:
                  terminationGracePeriodSeconds: 10
                  containers:
                  - name: cassandra
                    image: cassandra:latest
                    ports:
                    - containerPort: 9042
                      name: cassandra
                    resources:
                      requests:
                        cpu: "100m"
                        memory: 512Mi
                    env:
                      - name: MAX_HEAP_SIZE
                        value: 512M
                      - name: HEAP_NEWSIZE
                        value: 128M
                      - name: CASSANDRA_SNITCH
                        value: GossipingPropertyFileSnitch
                      - name: JVM_OPTS
                        value: "-Dcassandra.skip_wait_for_gossip_to_settle=0 -Dcassandra.initial_token=0"
                      - name: CASSANDRA_ENDPOINT_SNITCH
                        value: GossipingPropertyFileSnitch
                      - name:  CASSANDRA_DC
                        value: datacenter1
            """;

    @SneakyThrows
    protected static void installCassandra() {

        applyManifest(CASSANDRA_MANIFEST, namespace);

        final String cmd =
                "kubectl rollout status --watch --timeout=600s sts/cassandra -n %s"
                        .formatted(namespace);
        log.info("Running {}", cmd);
        runProcess(cmd.split(" "));

        log.info("Cassandra install completed");

        // Cassandra takes much time to boostrap
        Awaitility.await()
                .pollInterval(5, TimeUnit.SECONDS)
                .atMost(5, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            try {
                                execInPod(
                                                "cassandra-0",
                                                "cassandra",
                                                "cqlsh -e \"DESCRIBE keyspaces;\"")
                                        .get();
                            } catch (Throwable t) {
                                log.error("Failed to execute cqlsh command: {}", t.getMessage());
                                fail("Failed to execute cqlsh command: " + t.getMessage());
                            }
                        });

        log.info("Cassandra is up and running");
    }

    @SneakyThrows
    protected static String executeCQL(String command) {
        log.info("Executing CQL: {}", command);
        String somename = UUID.randomUUID() + ".txt";
        String podName = "cassandra-0";
        execInPod("cassandra-0", "cassandra", "echo  \"%s\" > /tmp/%s".formatted(command, somename))
                .get();
        String result =
                execInPod("cassandra-0", "cassandra", "cat /tmp/%s | cqlsh ".formatted(somename))
                        .get();
        log.info("CQL result: {}", result);
        return result;
    }

    @SneakyThrows
    protected static void uninstallCassandra() {
        deleteManifest(CASSANDRA_MANIFEST, namespace);
        log.info("Cassandra uninstall completed");
    }
}
