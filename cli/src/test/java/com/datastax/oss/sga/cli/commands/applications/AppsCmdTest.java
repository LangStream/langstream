package com.datastax.oss.sga.cli.commands.applications;

import static com.github.tomakehurst.wiremock.client.WireMock.aMultipart;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.BinaryEqualToPattern;
import com.github.tomakehurst.wiremock.matching.MatchResult;
import com.github.tomakehurst.wiremock.matching.MultipartValuePatternBuilder;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


class AppsCmdTest extends CommandTestBase {

    class InterceptDeploySupport {
        private final Path extract = Paths.get(tempDir.toFile().getAbsolutePath(), "extract");
        private final Path toExtract = Paths.get(tempDir.toFile().getAbsolutePath(), "toExtract");


        MultipartValuePatternBuilder multipartValuePatternBuilder() {
            return aMultipart()
                    .withName("file")
                    .withBody(new BinaryEqualToPattern("") {
                        @Override
                        @SneakyThrows
                        public MatchResult match(byte[] value) {
                            Files.write(extract, value);
                            new net.lingala.zip4j.ZipFile(extract.toFile()).extractAll(
                                    toExtract.toFile().getAbsolutePath());
                            return MatchResult.exactMatch();
                        }
                    });
        }

        ;

        @SneakyThrows
        public int countFiles() {
            return Files.list(toExtract).collect(Collectors.toList()).size();
        }

        @SneakyThrows
        public String getFileContent(String localZipName) {
            return Files.readString(Paths.get(toExtract.toFile().getAbsolutePath(), localZipName));
        }
    }

    @Test
    public void testDeploy() throws Exception {
        final String app = createTempFile("module: module-1");
        final String instance = createTempFile("instance: {}");
        final String secrets = createTempFile("secrets: []");

        final InterceptDeploySupport support = new InterceptDeploySupport();
        wireMock.register(WireMock.put("/api/applications/my-app")
                .withMultipartRequestBody(support.multipartValuePatternBuilder())
                .willReturn(WireMock.ok("{ \"name\": \"my-app\" }")));

        CommandResult result = executeCommand("apps", "deploy", "my-app", "-s", secrets, "-app", app, "-i", instance);
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());

        Assertions.assertEquals(3, support.countFiles());
        Assertions.assertEquals("module: module-1",
                support.getFileContent(new File(app).getName()));
        Assertions.assertEquals("instance: {}",
                support.getFileContent("instance.yaml"));
        Assertions.assertEquals("secrets: []",
                support.getFileContent("secrets.yaml"));

    }

    @Test
    public void testGet() throws Exception {
        wireMock.register(WireMock.get("/api/applications/my-app").willReturn(WireMock.ok("{ \"name\": \"my-app\" }")));

        CommandResult result = executeCommand("apps", "get", "my-app");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
        Assertions.assertEquals("{ \"name\": \"my-app\" }", result.out());

    }

    @Test
    public void testDelete() throws Exception {
        wireMock.register(WireMock.delete("/api/applications/my-app").willReturn(WireMock.ok()));

        CommandResult result = executeCommand("apps", "delete", "my-app");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
        Assertions.assertEquals("Application my-app deleted", result.out());

    }

    @Test
    public void testList() throws Exception {
        wireMock.register(WireMock.get("/api/applications").willReturn(WireMock.ok("{}")));

        CommandResult result = executeCommand("apps", "list");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
        Assertions.assertEquals("{}", result.out());

    }
}