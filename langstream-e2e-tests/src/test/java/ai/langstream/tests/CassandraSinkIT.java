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

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

@Slf4j
@ExtendWith(BaseEndToEndTest.class)
public class CassandraSinkIT extends BaseEndToEndTest {

    @BeforeAll
    public static void setupCassandra() {
        installCassandra();

        executeCQL("CREATE KEYSPACE IF NOT EXISTS vsearch WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };");
        executeCQL("CREATE TABLE IF NOT EXISTS vsearch.products (id int PRIMARY KEY,name TEXT,description TEXT);");
        executeCQL("SELECT * FROM vsearch.products;");
        executeCQL("INSERT INTO vsearch.products(id, name, description) VALUES (1, 'test', 'test');");
        executeCQL("SELECT * FROM vsearch.products;");
    }

    @AfterAll
    public static void removeCassandra() {
        uninstallCassandra();
    }

    @Test
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
        copyFileToClientContainer(Paths.get(testAppsBaseDir, "cassandra-sink").toFile(), "/tmp/cassandra-sink");
        copyFileToClientContainer(Paths.get(testInstanceBaseDir, "kafka-kubernetes.yaml").toFile(), "/tmp/instance.yaml");
        copyFileToClientContainer(Paths.get(testSecretBaseDir, "secret1.yaml").toFile(), "/tmp/secrets.yaml");


        executeCommandOnClient("bin/langstream apps deploy %s -app /tmp/cassandra-sink -i /tmp/instance.yaml -s /tmp/secrets.yaml".formatted(applicationId).split(" "));
        client.apps()
                .statefulSets()
                .inNamespace(TENANT_NAMESPACE_PREFIX + tenant)
                .withName(applicationId + "-module-1-pipeline-1-cassandra-sink-1")
                .waitUntilReady(4, TimeUnit.MINUTES);

        executeCommandOnClient("bin/langstream gateway produce %s produce-input -v '{\"id\": 10, \"name\": \"test\", \"description\": \"test\"}'".formatted(applicationId).split(" "));

        executeCQL("SELECT * FROM vsearch.products");

    }
}

