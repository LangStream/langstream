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

    protected WireMock wireMock;
    protected Path tempDir;
    private Path cliYaml;

    @BeforeEach
    public void beforeEach(WireMockRuntimeInfo wmRuntimeInfo, @TempDir Path tempDir) throws Exception {
        this.tempDir = tempDir;

        cliYaml = Path.of(tempDir.toFile().getAbsolutePath(), "cli.yaml");
        Files.write(cliYaml,
                "webServiceUrl: http://localhost:%d".formatted(wmRuntimeInfo.getHttpPort()).getBytes(
                        StandardCharsets.UTF_8));
        wireMock = wmRuntimeInfo.getWireMock();
    }

    protected String createTempFile(String content) {
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
