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
package ai.langstream.tests.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

import ai.langstream.tests.util.SystemOrEnv;
import ai.langstream.tests.util.TestSuites;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Slf4j
@Tag(TestSuites.CATEGORY_CLI)
public class CLIDockerRunIT {

    static String binaryLocation;
    static String configFile;
    static final List<CompletableFuture<?>> tasks = new ArrayList<>();
    static final List<Process> processes = new CopyOnWriteArrayList<>();

    @BeforeAll
    public static void beforeAll() {

        final String binary =
                SystemOrEnv.getProperty("LANGSTREAM_TESTS_CLI_BIN", "langstream.tests.cli.bin");
        if (binary == null) {
            binaryLocation = "langstream";
        } else {
            binaryLocation = binary;
        }
    }

    @BeforeEach
    public void setupTest() throws Exception {
        final Path configFile = Files.createTempFile("langstream", ".yaml");
        Files.writeString(
                configFile,
                """
                ---
                webServiceUrl: "http://localhost:8090"
                apiGatewayUrl: "ws://localhost:8091"
                tenant: "default"
                profiles:
                  local-docker-run:
                    webServiceUrl: "http://localhost:8090"
                    apiGatewayUrl: "ws://localhost:8091"
                    tenant: "default"
                    name: "local-docker-run"
                """);
        this.configFile = configFile.toFile().getAbsolutePath();
    }

    public void killProcess(ProcessHandle process) {
        // no need for recursion, spawned process must handle their own children
        process.children().forEach(p -> p.destroy());
        process.destroy();
    }

    @AfterEach
    public void afterEach() {
        processes.forEach(
                p -> {
                    log.info("destroy process {}", p);
                    killProcess(p.toHandle());
                });
        processes.clear();
        tasks.forEach(t -> t.cancel(true));
        tasks.clear();
    }

    private static CompletableFuture<Void> runCli(String args) {
        return runCli(args, null);
    }

    @SneakyThrows
    private static CompletableFuture<Void> runCli(String args, Map<String, String> env) {
        final List<String> allArgs = new ArrayList<>();
        if (env != null) {
            String beforeCmd =
                    env.entrySet().stream()
                            .map(
                                    e -> {
                                        final String safeValue =
                                                e.getValue() == null ? "" : e.getValue();
                                        return "export '%s'='%s'"
                                                .formatted(
                                                        e.getKey(), safeValue.replace("'", "''"));
                                    })
                            .collect(Collectors.joining(" && "));
            beforeCmd += " && ";
            allArgs.addAll(Arrays.stream(beforeCmd.split(" ")).toList());
        }
        allArgs.add(binaryLocation);
        allArgs.add("--conf");
        allArgs.add(configFile);
        allArgs.add("-v");
        allArgs.addAll(Arrays.stream(args.split(" ")).toList());
        final String fullCommand =
                allArgs.stream().filter(s -> !s.isBlank()).collect(Collectors.joining(" "));

        final CompletableFuture<Void> future =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                log.info("running command: {}", fullCommand);
                                final Process process =
                                        new ProcessBuilder("bash", "-c", fullCommand)
                                                .inheritIO()
                                                .redirectErrorStream(true)
                                                .start();
                                processes.add(process);
                                try {
                                    final int i = process.waitFor();
                                    if (i != 0) {
                                        throw new RuntimeException(
                                                "command failed: " + fullCommand);
                                    }
                                } catch (InterruptedException interruptedException) {
                                    log.info("interrupted");
                                    process.destroyForcibly();
                                    throw interruptedException;
                                }
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
        tasks.add(future);
        return future;
    }

    public static String localApp(String app) {
        return Path.of("src", "test", "resources", "apps", app).toFile().getAbsolutePath();
    }

    public static String localSecret() {
        return Path.of("src", "test", "resources", "secrets", "secret1.yaml")
                .toFile()
                .getAbsolutePath();
    }

    @Test
    public void test() throws Exception {
        final CompletableFuture<Void> future =
                runCli(
                        "docker run my-app -app %s -s %s --start-ui=false"
                                .formatted(localApp("python-processor"), localSecret()),
                        Map.of("SECRET1_VK", "super secret value"));

        final HttpClient client = HttpClient.newHttpClient();

        Awaitility.await()
                .pollInterval(1, TimeUnit.SECONDS)
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            final HttpRequest request =
                                    HttpRequest.newBuilder()
                                            .uri(
                                                    URI.create(
                                                            "http://localhost:8090/api/applications/default/my-app?stats=false"))
                                            .GET()
                                            .build();
                            try {
                                final HttpResponse<Void> response =
                                        client.send(
                                                request, HttpResponse.BodyHandlers.discarding());
                                assertEquals(200, response.statusCode());
                            } catch (Exception e) {
                                log.info(e.getMessage());
                                fail(e);
                            }
                        });
        runCli(
                        "gateway produce my-app produce-input -v my-value --connect-timeout 30 -p sessionId=s1")
                .get();
        runCli(
                        "gateway consume my-app consume-output --position earliest -n 1 --connect-timeout 30 -p sessionId=s1")
                .get();
        assertFalse(future.isDone());
    }
}
