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
package ai.langstream.cli.commands.applications;

import static ai.langstream.cli.utils.ApplicationPackager.buildZip;
import static com.github.tomakehurst.wiremock.client.WireMock.aMultipart;
import static com.github.tomakehurst.wiremock.client.WireMock.binaryEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.github.tomakehurst.wiremock.client.WireMock;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class AppsCmdTest extends CommandTestBase {

    @Test
    public void testDeploy() throws Exception {
        Path langstream = Files.createTempDirectory("langstream");
        final String app = createTempFile("module: module-1", langstream);
        final String instance = createTempFile("instance: {}");
        final String secrets = createTempFile("secrets: []");

        final Path zipFile = buildZip(langstream.toFile(), System.out::println);

        wireMock.register(
                WireMock.post(
                                String.format(
                                        "/api/applications/%s/my-app?dry-run=false&auto-upgrade=false",
                                        TENANT))
                        .withMultipartRequestBody(
                                aMultipart("app")
                                        .withBody(binaryEqualTo(Files.readAllBytes(zipFile))))
                        .withMultipartRequestBody(
                                aMultipart("instance").withBody(equalTo("instance: {}")))
                        .withMultipartRequestBody(
                                aMultipart("secrets").withBody(equalTo("secrets: []")))
                        .willReturn(WireMock.ok("{ \"name\": \"my-app\" }")));

        CommandResult result =
                executeCommand(
                        "apps",
                        "deploy",
                        "my-app",
                        "-s",
                        secrets,
                        "-app",
                        langstream.toAbsolutePath().toString(),
                        "-i",
                        instance);
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
    }

    @Test
    public void testDeployWithDependencies() throws Exception {

        final String fileContent = "dep-content";
        final String fileContentSha =
                "e1ebfd0f4e4a624eeeffc52c82b048739ea615dca9387630ae7767cb9957aa4ce2cf7afbd032ac8d5fcb73f42316655ea390e37399f14155ed794a6f53c066ec";
        wireMock.register(
                WireMock.get("/local/get-dependency.jar").willReturn(WireMock.ok(fileContent)));

        Path langstream = Files.createTempDirectory("langstream");
        Files.createDirectories(Path.of(langstream.toFile().getAbsolutePath(), "java", "lib"));
        final String configurationYamlContent =
                ("configuration:\n"
                        + "  dependencies:\n"
                        + "    - name: \"PostGRES JDBC Driver\"\n"
                        + "      url: \""
                        + wireMockBaseUrl
                        + "/local/get-dependency.jar\"\n"
                        + "      sha512sum: \""
                        + fileContentSha
                        + "\"\n"
                        + "      type: \"java-library\"\n");
        Files.writeString(
                Path.of(langstream.toFile().getAbsolutePath(), "configuration.yaml"),
                configurationYamlContent);
        Files.writeString(
                Path.of(langstream.toFile().getAbsolutePath(), "java", "lib", "get-dependency.jar"),
                fileContent);
        final String app = createTempFile("module: module-1", langstream);
        final String instance = createTempFile("instance: {}");
        final String secrets = createTempFile("secrets: []");

        final Path zipFile = buildZip(langstream.toFile(), System.out::println);
        wireMock.register(
                WireMock.post(
                                String.format(
                                        "/api/applications/%s/my-app?dry-run=false&auto-upgrade=false",
                                        TENANT))
                        .withMultipartRequestBody(
                                aMultipart("app")
                                        .withBody(binaryEqualTo(Files.readAllBytes(zipFile))))
                        .withMultipartRequestBody(
                                aMultipart("instance").withBody(equalTo("instance: {}")))
                        .withMultipartRequestBody(
                                aMultipart("secrets").withBody(equalTo("secrets: []")))
                        .willReturn(WireMock.ok("{ \"name\": \"my-app\" }")));

        CommandResult result =
                executeCommand(
                        "apps",
                        "deploy",
                        "my-app",
                        "-s",
                        secrets,
                        "-app",
                        langstream.toAbsolutePath().toString(),
                        "-i",
                        instance);
        Assertions.assertEquals("", result.err());
        Assertions.assertEquals(0, result.exitCode());
    }

    @Test
    public void testUpdateAll() throws Exception {
        Path langstream = Files.createTempDirectory("langstream");
        final String app = createTempFile("module: module-1", langstream);
        final String instance = createTempFile("instance: {}");
        final String secrets = createTempFile("secrets: []");

        final Path zipFile = buildZip(langstream.toFile(), System.out::println);
        wireMock.register(
                WireMock.patch(
                                urlEqualTo(
                                        String.format(
                                                "/api/applications/%s/my-app?auto-upgrade=false&force-restart=false",
                                                TENANT)))
                        .withMultipartRequestBody(
                                aMultipart("app")
                                        .withBody(binaryEqualTo(Files.readAllBytes(zipFile))))
                        .withMultipartRequestBody(
                                aMultipart("instance").withBody(equalTo("instance: {}")))
                        .withMultipartRequestBody(
                                aMultipart("secrets").withBody(equalTo("secrets: []")))
                        .willReturn(WireMock.ok("{ \"name\": \"my-app\" }")));

        CommandResult result =
                executeCommand(
                        "apps",
                        "update",
                        "my-app",
                        "-s",
                        secrets,
                        "-app",
                        langstream.toAbsolutePath().toString(),
                        "-i",
                        instance);
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
    }

    @Test
    public void testDeployDryRun() throws Exception {
        Path langstream = Files.createTempDirectory("langstream");
        final String app = createTempFile("module: module-1", langstream);
        final String instance = createTempFile("instance: {}");
        final String secrets = createTempFile("secrets: []");

        final Path zipFile = buildZip(langstream.toFile(), System.out::println);

        wireMock.register(
                WireMock.post(
                                String.format(
                                        "/api/applications/%s/my-app?dry-run=true&auto-upgrade=false",
                                        TENANT))
                        .withMultipartRequestBody(
                                aMultipart("app")
                                        .withBody(binaryEqualTo(Files.readAllBytes(zipFile))))
                        .withMultipartRequestBody(
                                aMultipart("instance").withBody(equalTo("instance: {}")))
                        .withMultipartRequestBody(
                                aMultipart("secrets").withBody(equalTo("secrets: []")))
                        .willReturn(WireMock.ok("{ \"name\": \"my-app\" }")));

        CommandResult result =
                executeCommand(
                        "apps",
                        "deploy",
                        "my-app",
                        "-s",
                        secrets,
                        "-app",
                        langstream.toAbsolutePath().toString(),
                        "-i",
                        instance,
                        "--dry-run");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
        Assertions.assertTrue(result.out().contains("name: \"my-app\""));

        result =
                executeCommand(
                        "apps",
                        "deploy",
                        "my-app",
                        "-s",
                        secrets,
                        "-app",
                        langstream.toAbsolutePath().toString(),
                        "-i",
                        instance,
                        "--dry-run",
                        "-o",
                        "json");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
        Assertions.assertTrue(result.out().contains("{\n" + "  \"name\" : \"my-app\"\n" + "}"));
    }

    @Test
    public void testDeployAutoUpgrade() throws Exception {
        Path langstream = Files.createTempDirectory("langstream");
        final String app = createTempFile("module: module-1", langstream);
        final String instance = createTempFile("instance: {}");
        final String secrets = createTempFile("secrets: []");

        final Path zipFile = buildZip(langstream.toFile(), System.out::println);

        wireMock.register(
                WireMock.post(
                                String.format(
                                        "/api/applications/%s/my-app?dry-run=false&auto-upgrade=true",
                                        TENANT))
                        .withMultipartRequestBody(
                                aMultipart("app")
                                        .withBody(binaryEqualTo(Files.readAllBytes(zipFile))))
                        .withMultipartRequestBody(
                                aMultipart("instance").withBody(equalTo("instance: {}")))
                        .withMultipartRequestBody(
                                aMultipart("secrets").withBody(equalTo("secrets: []")))
                        .willReturn(WireMock.ok("{ \"name\": \"my-app\" }")));

        CommandResult result =
                executeCommand(
                        "apps",
                        "deploy",
                        "my-app",
                        "-s",
                        secrets,
                        "-app",
                        langstream.toAbsolutePath().toString(),
                        "-i",
                        instance,
                        "--auto-upgrade");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
    }

    @Test
    public void testUpdateInstance() throws Exception {
        final String instance = createTempFile("instance: {}");

        wireMock.register(
                WireMock.patch(
                                urlEqualTo(
                                        String.format(
                                                "/api/applications/%s/my-app?auto-upgrade=false&force-restart=false",
                                                TENANT)))
                        .withMultipartRequestBody(
                                aMultipart("instance").withBody(equalTo("instance: {}")))
                        .willReturn(WireMock.ok("{ \"name\": \"my-app\" }")));

        CommandResult result = executeCommand("apps", "update", "my-app", "-i", instance);
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
    }

    @Test
    public void testUpdateAppAndInstance() throws Exception {
        Path langstream = Files.createTempDirectory("langstream");
        final String app = createTempFile("module: module-1", langstream);
        final String instance = createTempFile("instance: {}");

        final Path zipFile = buildZip(langstream.toFile(), System.out::println);
        wireMock.register(
                WireMock.patch(
                                urlEqualTo(
                                        String.format(
                                                "/api/applications/%s/my-app?auto-upgrade=false&force-restart=false",
                                                TENANT)))
                        .withMultipartRequestBody(
                                aMultipart("app")
                                        .withBody(binaryEqualTo(Files.readAllBytes(zipFile))))
                        .withMultipartRequestBody(
                                aMultipart("instance").withBody(equalTo("instance: {}")))
                        .willReturn(WireMock.ok("{ \"name\": \"my-app\" }")));

        CommandResult result =
                executeCommand(
                        "apps",
                        "update",
                        "my-app",
                        "-i",
                        instance,
                        "-app",
                        langstream.toFile().getAbsolutePath());
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
    }

    @Test
    public void testUpdateApp() throws Exception {
        Path langstream = Files.createTempDirectory("langstream");
        final String app = createTempFile("module: module-1", langstream);

        final Path zipFile = buildZip(langstream.toFile(), System.out::println);
        wireMock.register(
                WireMock.patch(
                                urlEqualTo(
                                        String.format(
                                                "/api/applications/%s/my-app?auto-upgrade=false&force-restart=false",
                                                TENANT)))
                        .withMultipartRequestBody(
                                aMultipart("app")
                                        .withBody(binaryEqualTo(Files.readAllBytes(zipFile))))
                        .willReturn(WireMock.ok("{ \"name\": \"my-app\" }")));

        CommandResult result =
                executeCommand(
                        "apps", "update", "my-app", "-app", langstream.toFile().getAbsolutePath());
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
    }

    @Test
    public void testUpdateAppWithAutoUpgrade() throws Exception {
        Path langstream = Files.createTempDirectory("langstream");
        final String app = createTempFile("module: module-1", langstream);

        final Path zipFile = buildZip(langstream.toFile(), System.out::println);
        wireMock.register(
                WireMock.patch(
                                urlEqualTo(
                                        String.format(
                                                "/api/applications/%s/my-app?auto-upgrade=true&force-restart=false",
                                                TENANT)))
                        .withMultipartRequestBody(
                                aMultipart("app")
                                        .withBody(binaryEqualTo(Files.readAllBytes(zipFile))))
                        .willReturn(WireMock.ok("{ \"name\": \"my-app\" }")));

        CommandResult result =
                executeCommand(
                        "apps",
                        "update",
                        "my-app",
                        "-app",
                        langstream.toFile().getAbsolutePath(),
                        "--auto-upgrade");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
    }

    @Test
    public void testUpdateAppWithForceRestart() throws Exception {
        Path langstream = Files.createTempDirectory("langstream");
        final String app = createTempFile("module: module-1", langstream);

        final Path zipFile = buildZip(langstream.toFile(), System.out::println);
        wireMock.register(
                WireMock.patch(
                                urlEqualTo(
                                        String.format(
                                                "/api/applications/%s/my-app?auto-upgrade=false&force-restart=true",
                                                TENANT)))
                        .withMultipartRequestBody(
                                aMultipart("app")
                                        .withBody(binaryEqualTo(Files.readAllBytes(zipFile))))
                        .willReturn(WireMock.ok("{ \"name\": \"my-app\" }")));

        CommandResult result =
                executeCommand(
                        "apps",
                        "update",
                        "my-app",
                        "-app",
                        langstream.toFile().getAbsolutePath(),
                        "--force-restart");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
    }

    @Test
    public void testUpdateSecrets() throws Exception {
        final String secrets = createTempFile("secrets: []");
        wireMock.register(
                WireMock.patch(
                                urlEqualTo(
                                        String.format(
                                                "/api/applications/%s/my-app?auto-upgrade=false&force-restart=false",
                                                TENANT)))
                        .withMultipartRequestBody(
                                aMultipart("secrets").withBody(equalTo("secrets: []")))
                        .willReturn(WireMock.ok("{ \"name\": \"my-app\" }")));

        CommandResult result = executeCommand("apps", "update", "my-app", "-s", secrets);
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
    }

    @Test
    public void testGet() throws Exception {
        final String jsonValue =
                new String(
                        AppsCmdTest.class
                                .getClassLoader()
                                .getResourceAsStream("expected-get.json")
                                .readAllBytes(),
                        StandardCharsets.UTF_8);
        wireMock.register(
                WireMock.get(String.format("/api/applications/%s/my-app?stats=false", TENANT))
                        .willReturn(WireMock.ok(jsonValue)));

        CommandResult result = executeCommand("apps", "get", "my-app");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
        Assertions.assertEquals(
                "ID          STREAMING   COMPUTE     STATUS      EXECUTORS   REPLICAS  \n"
                        + "test        kafka       kubernetes  DEPLOYED    2/2         2/2",
                result.out());
        ObjectMapper jsonPrinter =
                new ObjectMapper()
                        .enable(SerializationFeature.INDENT_OUTPUT)
                        .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
        result = executeCommand("apps", "get", "my-app", "-o", "json");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());

        final String expectedJson =
                jsonPrinter.writeValueAsString(jsonPrinter.readValue(jsonValue, JsonNode.class));
        Assertions.assertEquals(expectedJson, result.out());

        final ObjectMapper yamlPrinter =
                new ObjectMapper(new YAMLFactory())
                        .enable(SerializationFeature.INDENT_OUTPUT)
                        .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
        final String expectedYaml =
                yamlPrinter.writeValueAsString(jsonPrinter.readValue(jsonValue, JsonNode.class));

        result = executeCommand("apps", "get", "my-app", "-o", "yaml");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
        Assertions.assertEquals(expectedYaml.strip(), result.out());

        result = executeCommand("apps", "get", "my-app", "-o", "mermaid");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
        Assertions.assertNotEquals("", result.out());
    }

    @Test
    public void testDelete() {
        wireMock.register(
                WireMock.delete(String.format("/api/applications/%s/my-app?force=false", TENANT))
                        .willReturn(WireMock.ok()));

        CommandResult result = executeCommand("apps", "delete", "my-app");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
        Assertions.assertEquals("Application 'my-app' marked for deletion", result.out());
    }

    @Test
    public void testForceDelete() {
        wireMock.register(
                WireMock.delete(String.format("/api/applications/%s/my-app?force=true", TENANT))
                        .willReturn(WireMock.ok()));

        CommandResult result = executeCommand("apps", "delete", "my-app", "-f");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
        Assertions.assertEquals("Application 'my-app' marked for deletion (forced)", result.out());
    }

    @Test
    public void testList() {
        wireMock.register(
                WireMock.get(String.format("/api/applications/%s", TENANT))
                        .willReturn(WireMock.ok("[]")));

        CommandResult result = executeCommand("apps", "list");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
        Assertions.assertEquals(
                "ID         STREAMING  COMPUTE    STATUS     EXECUTORS  REPLICAS", result.out());
        result = executeCommand("apps", "list", "-o", "json");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
        Assertions.assertEquals("[ ]", result.out());
        result = executeCommand("apps", "list", "-o", "yaml");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
        Assertions.assertEquals("--- []", result.out());
    }

    @Test
    public void testLogs() {
        wireMock.register(
                WireMock.get(String.format("/api/applications/%s/my-app/logs?format=text", TENANT))
                        .willReturn(WireMock.ok("some logs")));

        CommandResult result = executeCommand("apps", "logs", "my-app");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
        Assertions.assertEquals("some logs", result.out());

        wireMock.register(
                WireMock.get(String.format("/api/applications/%s/my-app/logs?format=json", TENANT))
                        .willReturn(
                                WireMock.ok(
                                        "{\"replica\":\"app-1-0\","
                                                + "\"message\":\"08:53:44.519 [stats-pipeline-python-processor-1] INFO  a.l.runtime.agent.AgentRunner -- Records: total 5, working 0, Memory stats: used 16 MB, total 38 MB, free 22 MB, max 121 MB Direct memory 4 MB\",\"timestamp\":1697447779798}")));

        result = executeCommand("apps", "logs", "my-app", "-o", "json");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
        Assertions.assertEquals(
                "{\"replica\":\"app-1-0\",\"message\":\"08:53:44.519 "
                        + "[stats-pipeline-python-processor-1] INFO  a.l.runtime.agent.AgentRunner -- "
                        + "Records: total 5, working 0, Memory stats: used 16 MB, total 38 MB, free 22 MB, "
                        + "max 121 MB Direct memory 4 MB\",\"timestamp\":1697447779798}",
                result.out());
    }

    @Test
    public void testDownload() {
        wireMock.register(
                WireMock.get(String.format("/api/applications/%s/my-app/code", TENANT))
                        .willReturn(WireMock.ok()));

        final File expectedFile = new File(String.format("%s-my-app.zip", TENANT));
        try {
            CommandResult result = executeCommand("apps", "download", "my-app");
            Assertions.assertEquals(0, result.exitCode());
            Assertions.assertEquals("", result.err());
            Assertions.assertEquals(
                    "Downloaded application code to " + expectedFile.getAbsolutePath(),
                    result.out());
        } finally {
            try {
                Files.deleteIfExists(expectedFile.toPath());
            } catch (IOException e) {
            }
        }
    }

    @Test
    public void testDownloadToFile() {
        wireMock.register(
                WireMock.get(String.format("/api/applications/%s/my-app/code", TENANT))
                        .willReturn(WireMock.ok()));

        CommandResult result =
                executeCommand("apps", "download", "my-app", "-o", "/tmp/download.zip");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
        Assertions.assertEquals("Downloaded application code to /tmp/download.zip", result.out());
    }

    @Test
    public void testDeployWithFilePlaceholders() throws Exception {
        Path langstream = Files.createTempDirectory("langstream");
        final String instance = createTempFile("instance: {}");
        final String jsonFileRelative =
                Paths.get(createTempFile("{\"client-id\":\"xxx\"}")).getFileName().toString();
        assertFalse(jsonFileRelative.contains("/"));

        final String secrets =
                createTempFile(
                        String.format(
                                ("secrets:\n"
                                        + "     - name: vertex-ai\n"
                                        + "       id: vertex-ai\n"
                                        + "       data:\n"
                                        + "         url: https://us-central1-aiplatform.googleapis.com\n"
                                        + "         token: xxx\n"
                                        + "         serviceAccountJson: \"<file:%s>\"\n"
                                        + "         region: us-central1\n"
                                        + "         project: myproject\n"),
                                jsonFileRelative));

        final Path zipFile = buildZip(langstream.toFile(), System.out::println);

        wireMock.register(
                WireMock.post(
                                String.format(
                                        "/api/applications/%s/my-app?dry-run=false&auto-upgrade=false",
                                        TENANT))
                        .withMultipartRequestBody(
                                aMultipart("app")
                                        .withBody(binaryEqualTo(Files.readAllBytes(zipFile))))
                        .withMultipartRequestBody(
                                aMultipart("instance").withBody(equalTo("instance: {}")))
                        .withMultipartRequestBody(
                                aMultipart("secrets")
                                        .withBody(
                                                equalTo(
                                                        "---\n"
                                                                + "secrets:\n"
                                                                + "- name: \"vertex-ai\"\n"
                                                                + "  id: \"vertex-ai\"\n"
                                                                + "  data:\n"
                                                                + "    url: \"https://us-central1-aiplatform.googleapis.com\"\n"
                                                                + "    token: \"xxx\"\n"
                                                                + "    serviceAccountJson: \"{\\\"client-id\\\":\\\"xxx\\\"}\"\n"
                                                                + "    region: \"us-central1\"\n"
                                                                + "    project: \"myproject\"\n")))
                        .willReturn(WireMock.ok("{ \"name\": \"my-app\" }")));

        CommandResult result =
                executeCommand(
                        "apps",
                        "deploy",
                        "my-app",
                        "-s",
                        secrets,
                        "-app",
                        langstream.toAbsolutePath().toString(),
                        "-i",
                        instance);
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
    }
}
