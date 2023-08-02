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
package com.datastax.oss.sga.cli.commands.applications;

import static org.junit.jupiter.api.Assertions.assertEquals;
import com.datastax.oss.sga.cli.SgaCLI;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

@Slf4j
@WireMockTest
public class CommandTestBase {

    protected static final String TENANT = "my-tenant";
    protected WireMock wireMock;
    protected Path tempDir;
    private Path cliYaml;

    @BeforeEach
    public void beforeEach(WireMockRuntimeInfo wmRuntimeInfo, @TempDir Path tempDir) throws Exception {
        this.tempDir = tempDir;

        cliYaml = Path.of(tempDir.toFile().getAbsolutePath(), "cli.yaml");
        final String config = """
                webServiceUrl: http://localhost:%d
                tenant: %s
                """.formatted(wmRuntimeInfo.getHttpPort(), TENANT);
        Files.write(cliYaml, config.getBytes(StandardCharsets.UTF_8));
        wireMock = wmRuntimeInfo.getWireMock();
    }

    protected String createTempFile(String content) {
        return createTempFile(content, tempDir);
    }

    protected String createTempFile(String content, Path tempDir) {
        try {
            Path tempFile = Files.createTempFile(tempDir, "sga-cli-test", ".yaml");
            Files.write(tempFile, content.getBytes(StandardCharsets.UTF_8));
            return tempFile.toFile().getAbsolutePath();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public record CommandResult(int exitCode, String out, String err) {
    }


    protected CommandResult executeCommand(String... args) {
        final String[] fullArgs = Stream.concat(
                        Arrays.stream(new String[]{"--conf", cliYaml.toFile().getAbsolutePath()}),
                        Arrays.stream(args))
                .toArray(String[]::new);
        log.info("executing command: {}", Arrays.toString(fullArgs));
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream oldErr = System.err;
        PrintStream oldOut = System.out;
        int exitCode = Integer.MIN_VALUE;
        try {
            System.setErr(new PrintStream(err));
            System.setOut(new PrintStream(out));
            exitCode = SgaCLI.execute(fullArgs);
        } finally {
            System.setErr(oldErr);
            System.setOut(oldOut);
        }
        String errRes = err.toString().trim().stripTrailing();
        if (!errRes.isBlank()) {
            log.error("COMMAND ERROR OUTPUT:\n");
            log.error(err.toString().trim());
        }
        String outRes = out.toString().stripTrailing();
        log.info("COMMAND OUTPUT:\n");
        log.info(outRes);
        return new CommandResult(exitCode, outRes, errRes);
    }


}
