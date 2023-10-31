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
package ai.langstream.cli.commands.python;

import ai.langstream.cli.util.DockerImageUtils;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import lombok.SneakyThrows;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;
import picocli.CommandLine;

@CommandLine.Command(
        name = "load-pip-requirements",
        header = "Process python dependencies in requirements.txt")
public class LoadPythonDependenciesCmd extends BasePythonCmd {

    private static final AtomicReference<ProcessHandle> dockerProcess = new AtomicReference<>();

    @CommandLine.Option(
            names = {"-app", "--application"},
            description = "Application directory path",
            required = true)
    private String appPath;

    @CommandLine.Option(
            names = {"--docker-args"},
            description = "Additional docker arguments")
    private List<String> dockerAdditionalArgs = new ArrayList<>();

    @CommandLine.Option(
            names = {"--docker-command"},
            description = "Command to run docker")
    private String dockerCommand = "docker";

    @CommandLine.Option(
            names = {"--langstream-runtime-version"},
            description = "Version of the LangStream runtime to use",
            defaultValue = "${env:LANGSTREAM_RUNTIME_DOCKER_IMAGE_VERSION}")
    private String dockerImageVersion;

    @CommandLine.Option(
            names = {"--langstream-runtime-docker-image"},
            description = "Docker image of the LangStream runtime to use",
            defaultValue = "${env:LANGSTREAM_RUNTIME_DOCKER_IMAGE}")
    private String dockerImageName;

    @Override
    @SneakyThrows
    public void run() {

        DockerImageUtils.DockerImage dockerImage =
                DockerImageUtils.computeDockerImage(dockerImageVersion, dockerImageName);

        if (appPath == null || appPath.isEmpty()) {
            throw new IllegalArgumentException("application files are required");
        }

        final File appDirectory = new File(appPath);

        log("Using docker image: " + dockerImage.getFullName());

        downloadDependencies(appDirectory.toPath(), getClient(), this::log);

        Runtime.getRuntime().addShutdownHook(new Thread(this::cleanEnvironment));

        executeOnDocker(appDirectory, dockerImage);
    }

    private void cleanEnvironment() {
        if (dockerProcess.get() != null) {
            dockerProcess.get().destroyForcibly();
        }
    }

    private void executeOnDocker(File appDirectory, DockerImageUtils.DockerImage dockerImage)
            throws Exception {
        final File appTmp = appDirectory;

        File pythonDirectory = new File(appDirectory, "python");
        if (!pythonDirectory.isDirectory()) {
            throw new IllegalArgumentException(
                    "Directory " + pythonDirectory.getAbsolutePath() + " not found");
        }
        File requirementsFile = new File(pythonDirectory, "requirements.txt");
        if (!requirementsFile.isFile()) {
            throw new IllegalArgumentException(
                    "File "
                            + requirementsFile.getAbsolutePath()
                            + " not found in "
                            + pythonDirectory);
        }

        String imageName = dockerImage.getFullName();
        List<String> commandLine = new ArrayList<>();
        commandLine.add(dockerCommand);

        /*
        docker run --rm \
        -v $(pwd):/app-code-download \
        --entrypoint "" \
        -w /app-code-download/python ghcr.io/langstream/langstream-runtime:0.1.0 \
                /bin/bash -c 'pip3 install --target ./lib --upgrade --prefer-binary -r requirements.txt'

         */

        commandLine.add("run");
        commandLine.add("--rm");
        commandLine.add("--entrypoint");
        commandLine.add("/bin/bash");
        commandLine.add("-w");
        commandLine.add("/code/application/python");

        commandLine.add("-v");
        commandLine.add(appTmp.getAbsolutePath() + ":/code/application");

        if (dockerAdditionalArgs != null) {
            commandLine.addAll(dockerAdditionalArgs);
        }

        commandLine.add(imageName);

        if (getRootCmd().isVerbose()) {
            System.out.println("Executing:");
            System.out.println(String.join(" ", commandLine));
        }

        commandLine.add("-c");
        commandLine.add(
                "pip3 install --target ./lib --upgrade --prefer-binary -r requirements.txt");

        final Path outputLog = Files.createTempFile("langstream", ".log");
        log("Logging to file: " + outputLog.toAbsolutePath());
        ProcessBuilder processBuilder =
                new ProcessBuilder(commandLine)
                        .redirectErrorStream(true)
                        .redirectOutput(outputLog.toFile());
        Process process = processBuilder.start();
        dockerProcess.set(process.toHandle());
        CompletableFuture.runAsync(
                () -> tailLogSysOut(outputLog), Executors.newSingleThreadExecutor());

        final int exited = process.waitFor();
        // wait for the log to be printed
        Thread.sleep(1000);
        if (exited != 0) {
            throw new RuntimeException("Process exited with code " + exited);
        }
    }

    private void tailLogSysOut(Path outputLog) {

        TailerListener listener =
                new TailerListener() {
                    @Override
                    public void fileNotFound() {}

                    @Override
                    public void fileRotated() {}

                    @Override
                    public void handle(Exception e) {}

                    @Override
                    public void handle(String s) {
                        log(s);
                    }

                    @Override
                    public void init(Tailer tailer) {}
                };
        try (final Tailer tailer =
                Tailer.builder()
                        .setTailerListener(listener)
                        .setStartThread(false)
                        .setDelayDuration(Duration.ofMillis(100))
                        .setFile(outputLog.toFile())
                        .get(); ) {
            while (true) {
                tailer.run();
            }
        }
    }
}
