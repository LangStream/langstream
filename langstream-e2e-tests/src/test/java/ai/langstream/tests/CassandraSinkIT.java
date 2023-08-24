/**
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

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Slf4j
@ExtendWith(BaseEndToEndTest.class)
public class CassandraSinkIT extends BaseEndToEndTest {

    // @BeforeAll
    public static void setupCassandra() {
        installCassandra();

        executeCQL("CREATE KEYSPACE IF NOT EXISTS vsearch WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };");
        executeCQL("CREATE TABLE IF NOT EXISTS vsearch.products (id int PRIMARY KEY,name TEXT,description TEXT);");
        executeCQL("SELECT * FROM vsearch.products;");
        executeCQL("INSERT INTO vsearch.products(id, name, description) VALUES (1, 'test-init', 'test-init');");
        executeCQL("SELECT * FROM vsearch.products;");
    }

    // @AfterAll
    public static void removeCassandra() {
        uninstallCassandra();
    }

    // @Test
    public void test() throws Exception {

        final String tenant = "ten-" + System.currentTimeMillis();
        executeCommandOnClient("""
                bin/langstream tenants put %s && 
                bin/langstream configure tenant %s""".formatted(
                tenant,
                tenant).replace(System.lineSeparator(), " ").split(" "));
        String testAppsBaseDir = "src/test/resources/apps";
        String testInstanceBaseDir = "src/test/resources/instances";
        String testSecretBaseDir = "src/test/resources/secrets";
        final String applicationId = "my-test-app";
        String cassandraHost = "cassandra-0.cassandra." + namespace;
        copyFileToClientContainer(Paths.get(testAppsBaseDir, "cassandra-sink").toFile(), "/tmp/cassandra-sink");
        copyFileToClientContainer(Paths.get(testInstanceBaseDir, "kafka-kubernetes.yaml").toFile(), "/tmp/instance.yaml");
        copyFileToClientContainer(Paths.get(testSecretBaseDir, "secret1.yaml").toFile(), "/tmp/secrets.yaml", file -> file.replace("CASSANDRA-HOST-INJECTED", cassandraHost));

        executeCommandOnClient("bin/langstream apps deploy %s -app /tmp/cassandra-sink -i /tmp/instance.yaml -s /tmp/secrets.yaml".formatted(applicationId).split(" "));
        client.apps()
                .statefulSets()
                .inNamespace(TENANT_NAMESPACE_PREFIX + tenant)
                .withName(applicationId + "-module-1-pipeline-1-sink-1")
                .waitUntilReady(4, TimeUnit.MINUTES);

        executeCommandOnClient("bin/langstream gateway produce %s produce-input -v '{\"id\": 10, \"name\": \"test-from-sink\", \"description\": \"test-from-sink\"}'".formatted(applicationId).split(" "));

        Awaitility.await().untilAsserted(() -> {
            String contents = executeCQL("SELECT * FROM vsearch.products;");
            assertTrue(contents.contains("test-from-sink"));
        });

    }


    private static final String CASSANDRA_MANIFEST = """
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
                        value: 100M
            """;

    @SneakyThrows
    protected static void installCassandra() {

        final Path tempFile = Files.createTempFile("cassandra-test", ".yaml");
        Files.write(tempFile, CASSANDRA_MANIFEST.getBytes(StandardCharsets.UTF_8));

        String cmd = "kubectl apply -n %s -f %s".formatted(namespace, tempFile.toFile().getAbsolutePath());
        log.info("Running {}", cmd);
        runProcess(cmd.split(" "));

        applyManifest(CASSANDRA_MANIFEST, namespace);

        cmd = "kubectl rollout status --watch --timeout=600s sts/cassandra -n %s"
                .formatted(namespace);
        log.info("Running {}", cmd);
        runProcess(cmd.split(" "));


        log.info("Cassandra install completed");

        // Cassandra takes much time to boostrap
        Awaitility.await()
                .pollInterval(5, TimeUnit.SECONDS)
                .atMost(5, TimeUnit.MINUTES).untilAsserted(() -> {
                    try {
                        execInPod("cassandra-0", "cassandra","cqlsh -e \"DESCRIBE keyspaces;\"").get();
                    } catch (Throwable t) {
                        log.error("Failed to execute cqlsh command: {}", t.getMessage());
                        fail("Failed to execute cqlsh command: "+ t.getMessage());
                    }
                });

        log.info("Cassandra is up and running");

    }

    @SneakyThrows
    protected static String  executeCQL(String command) {
        log.info("Executing CQL: {}", command);
        String somename = UUID.randomUUID() + ".txt";
        String podName = "cassandra-0";
        execInPod("cassandra-0", "cassandra", "echo  \"%s\" > /tmp/%s".formatted(command, somename)).get();
        String result  = execInPod("cassandra-0", "cassandra", "cat /tmp/%s | cqlsh ".formatted(somename)).get();
        log.info("CQL result: {}", result);
        return result;
    }

    @SneakyThrows
    protected static void uninstallCassandra() {

        final Path tempFile = Files.createTempFile("cassandra-test", ".yaml");
        Files.write(tempFile, CASSANDRA_MANIFEST.getBytes(StandardCharsets.UTF_8));

        final String cmd = "kubectl delete -n %s -f %s".formatted(namespace, tempFile.toFile().getAbsolutePath());
        log.info("Running {}", cmd);
        runProcess(cmd.split(" "));
        log.info("Cassandra uninstall completed");

    }

}

