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
package ai.langstream.cli.commands.docker;

import static org.junit.jupiter.api.Assertions.*;

import ai.langstream.cli.NamedProfile;
import ai.langstream.cli.commands.applications.CommandTestBase;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class LocalRunApplicationCmdTest extends CommandTestBase {

    @Test
    void testArgs() throws Exception {
        final Path tempDir = Files.createTempDirectory(this.tempDir, "langstream");

        final String appDir = tempDir.toFile().getAbsolutePath();
        CommandResult result =
                executeCommand(
                        "docker", "run", "my-app", "-app", appDir, "--docker-command", "echo");
        assertEquals("", result.err());
        assertEquals(0, result.exitCode());

        final List<String> lines = result.out().lines().collect(Collectors.toList());
        final String lastLine = lines.get(lines.size() - 1);
        assertTrue(
                lastLine.contains(
                        "run --rm -i -e START_BROKER=true -e START_MINIO=true -e START_HERDDB=true "
                                + "-e LANSGSTREAM_TESTER_TENANT=local-docker-run -e LANSGSTREAM_TESTER_APPLICATIONID=my-app "
                                + "-e LANSGSTREAM_TESTER_STARTWEBSERVICES=true -e LANSGSTREAM_TESTER_DRYRUN=false "
                                + "-v "
                                + appDir
                                + ":/code/application "));
        assertTrue(
                lastLine.contains(
                        "--add-host minio.minio-dev.svc.cluster.local:127.0.0.1 "
                                + "--add-host herddb.herddb-dev.svc.cluster.local:127.0.0.1 "
                                + "--add-host my-cluster-kafka-bootstrap.kafka:127.0.0.1 "
                                + "-p 8091:8091 "
                                + "-p 8090:8090 "
                                + "ghcr.io/langstream/langstream-runtime-tester:unknown"));

        final NamedProfile namedProfile = getConfig().getProfiles().get("local-docker-run");
        assertNotNull(namedProfile);
        assertEquals("local-docker-run", namedProfile.getTenant());
        assertEquals("http://localhost:8090", namedProfile.getWebServiceUrl());
        assertEquals("ws://localhost:8091", namedProfile.getApiGatewayUrl());
    }
}
