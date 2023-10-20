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
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class LocalRunApplicationCmdTest extends CommandTestBase {

    @Test
    void testArgs() throws Exception {
        final Path tempDir = Files.createTempDirectory(this.tempDir, "langstream");
        final Path appConfigFile = Files.createTempFile(tempDir, "configuration", ".yaml");
        Files.writeString(appConfigFile, "configuration: {}");

        final Path secrets = Files.createTempFile("langstream", ".yaml");
        Files.writeString(secrets, "secrets: []");

        final String appDir = tempDir.toFile().getAbsolutePath();
        CommandResult result =
                executeCommand(
                        "docker",
                        "run",
                        "my-app",
                        "-app",
                        appDir,
                        "-s",
                        secrets.toFile().getAbsolutePath(),
                        "--docker-command",
                        "echo");
        assertEquals("", result.err());
        assertEquals(0, result.exitCode());

        final List<String> lines = result.out().lines().collect(Collectors.toList());
        final String lastLine = lines.get(lines.size() - 1);
        assertTrue(
                lastLine.contains(
                        "run --rm -i -e START_BROKER=true -e START_MINIO=true -e START_HERDDB=true "
                                + "-e LANSGSTREAM_TESTER_TENANT=default -e LANSGSTREAM_TESTER_APPLICATIONID=my-app "
                                + "-e LANSGSTREAM_TESTER_STARTWEBSERVICES=true -e LANSGSTREAM_TESTER_DRYRUN=false "));
        assertTrue(
                lastLine.contains(
                        "--add-host minio.minio-dev.svc.cluster.local:127.0.0.1 "
                                + "--add-host herddb.herddb-dev.svc.cluster.local:127.0.0.1 "
                                + "--add-host my-cluster-kafka-bootstrap.kafka:127.0.0.1 "
                                + "-p 8091:8091 "
                                + "-p 8090:8090 "
                                + "ghcr.io/langstream/langstream-runtime-tester:unknown"));

        final List<String> volumes = extractVolumes(lastLine);
        assertEquals(3, volumes.size());
        volumes.forEach(
                volume -> {
                    final String hostPath = volume.split(":")[0];
                    final File file = new File(hostPath);
                    assertTrue(file.exists());
                    final Path langstreamTmp =
                            Path.of(System.getProperty("user.home"), ".langstream", "tmp");
                    assertEquals(langstreamTmp, file.toPath().getParent());
                    final Set<PosixFilePermission> posixFilePermissions;
                    try {
                        posixFilePermissions = Files.getPosixFilePermissions(file.toPath());
                        if (file.getName().contains("app")) {
                            assertEquals(
                                    Set.of(
                                            PosixFilePermission.OWNER_READ,
                                            PosixFilePermission.OWNER_WRITE,
                                            PosixFilePermission.GROUP_READ,
                                            PosixFilePermission.OTHERS_READ,
                                            PosixFilePermission.OWNER_EXECUTE,
                                            PosixFilePermission.GROUP_EXECUTE,
                                            PosixFilePermission.OTHERS_EXECUTE),
                                    posixFilePermissions);
                            assertTrue(file.isDirectory());
                            final String[] children = file.list();
                            assertEquals(1, children.length);
                            assertFileReadable(
                                    Files.getPosixFilePermissions(
                                            Path.of(file.getAbsolutePath(), children[0])));
                        } else {
                            assertFileReadable(posixFilePermissions);
                        }

                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });

        final NamedProfile namedProfile = getConfig().getProfiles().get("local-docker-run");
        assertNotNull(namedProfile);
        assertEquals("default", namedProfile.getTenant());
        assertEquals("http://localhost:8090", namedProfile.getWebServiceUrl());
        assertEquals("ws://localhost:8091", namedProfile.getApiGatewayUrl());
    }

    private void assertFileReadable(Set<PosixFilePermission> posixFilePermissions) {
        assertEquals(
                Set.of(
                        PosixFilePermission.OWNER_READ,
                        PosixFilePermission.OWNER_WRITE,
                        PosixFilePermission.GROUP_READ,
                        PosixFilePermission.OTHERS_READ),
                posixFilePermissions);
    }

    private static List<String> extractVolumes(String input) {
        List<String> volumes = new ArrayList<>();
        Pattern pattern = Pattern.compile("-v\\s+([^\\s]+)");
        Matcher matcher = pattern.matcher(input);

        while (matcher.find()) {
            volumes.add(matcher.group(1));
        }

        return volumes;
    }
}
