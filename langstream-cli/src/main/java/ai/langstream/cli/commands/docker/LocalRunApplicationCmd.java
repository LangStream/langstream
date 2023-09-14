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

import ai.langstream.cli.util.LocalFileReferenceResolver;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import lombok.SneakyThrows;
import picocli.CommandLine;

@CommandLine.Command(name = "run", header = "Run on a docker container a LangStream application")
public class LocalRunApplicationCmd extends BaseDockerCmd {

    @CommandLine.Parameters(description = "Name of the application")
    private String name;

    @CommandLine.Option(
            names = {"-app", "--application"},
            description = "Application directory path",
            required = true)
    private String appPath;

    @CommandLine.Option(
            names = {"-i", "--instance"},
            description = "Instance file path",
            required = true)
    private String instanceFilePath;

    @CommandLine.Option(
            names = {"--start-broker"},
            description = "Start the broker")
    private boolean startBroker = true;

    @CommandLine.Option(
            names = {"--start-s3"},
            description = "Start the S3 service")
    private boolean startS3 = true;

    @CommandLine.Option(
            names = {"--start-webservices"},
            description = "Start LangStream webservices")
    private boolean startWebservices = true;

    @CommandLine.Option(
            names = {"--only-agent"},
            description = "Run only one agent")
    private String singleAgentId;

    @CommandLine.Option(
            names = {"-s", "--secrets"},
            description = "Secrets file path")
    private String secretFilePath;

    @CommandLine.Option(
            names = {"--memory"},
            description = "Memory for the docker container")
    private String memory;

    @CommandLine.Option(
            names = {"--cpus"},
            description = "CPU for the docker container")
    private String cpus;

    @CommandLine.Option(
            names = {"--docker-args"},
            description = "Additional docker arguments")
    private List<String> dockerAdditionalArgs = new ArrayList<>();

    @CommandLine.Option(
            names = {"--docker-command"},
            description = "Command to run docker")
    private String dockerCommand = "docker";

    @Override
    @SneakyThrows
    public void run() {
        final File appDirectory = checkFileExistsOrDownload(appPath);
        final File instanceFile = checkFileExistsOrDownload(instanceFilePath);
        final File secretsFile = checkFileExistsOrDownload(secretFilePath);

        log("Tenant " + getTenant());
        log("Application " + name);
        log("Application directory: " + appDirectory.getAbsolutePath());
        if (singleAgentId != null && !singleAgentId.isEmpty()) {
            log("Filter agent: " + singleAgentId);
        } else {
            log("Running all the agents in the application");
        }
        log("Instance file: " + instanceFile.getAbsolutePath());
        log("Secrets file: " + secretsFile.getAbsolutePath());
        log("Start broker: " + startBroker);
        log("Start S3: " + startS3);
        log("Start Webservices " + startWebservices);

        if ((appDirectory == null || instanceFile == null)) {
            throw new IllegalArgumentException("application and instance files are required");
        }

        downloadDependencies(appDirectory.toPath());

        String secretsContents;
        String instanceContents;

        try {

            instanceContents =
                    LocalFileReferenceResolver.resolveFileReferencesInYAMLFile(
                            instanceFile.toPath());
        } catch (Exception e) {
            log(
                    "Failed to resolve instance file references. Please double check the file path: "
                            + instanceFile.toPath());
            e.printStackTrace();
            throw e;
        }

        try {

            secretsContents =
                    LocalFileReferenceResolver.resolveFileReferencesInYAMLFile(
                            secretsFile.toPath());
        } catch (Exception e) {
            log(
                    "Failed to resolve secrets file references. Please double check the file path: "
                            + secretsFile.toPath());
            throw e;
        }

        executeOnDocker(
                getTenant(),
                name,
                singleAgentId,
                appDirectory,
                instanceContents,
                secretsContents,
                startBroker,
                startS3,
                startWebservices);
    }

    private void executeOnDocker(
            String tenant,
            String applicationId,
            String singleAgentId,
            File appDirectory,
            String instanceContents,
            String secretsContents,
            boolean startBroker,
            boolean startS3,
            boolean startWebservices)
            throws Exception {
        File tmpInstanceFile = Files.createTempFile("instance", ".yaml").toFile();
        Files.write(tmpInstanceFile.toPath(), instanceContents.getBytes(StandardCharsets.UTF_8));
        File tmpSecretsFile = Files.createTempFile("secrets", ".yaml").toFile();
        Files.write(tmpSecretsFile.toPath(), secretsContents.getBytes(StandardCharsets.UTF_8));
        String imageName = "langstream/langstream-runtime-tester:latest-dev";
        List<String> commandLine = new ArrayList<>();
        commandLine.add(dockerCommand);
        commandLine.add("run");
        commandLine.add("--rm");
        commandLine.add("-it");
        commandLine.add("-e");
        commandLine.add("START_BROKER=" + startBroker);
        commandLine.add("-e");
        commandLine.add("START_S3=" + startS3);
        commandLine.add("-e");
        commandLine.add("LANSGSTREAM_TESTER_TENANT=" + tenant);
        commandLine.add("-e");
        commandLine.add("LANSGSTREAM_TESTER_APPLICATIONID=" + applicationId);
        commandLine.add("-e");
        commandLine.add("LANSGSTREAM_TESTER_STARTWEBSERVICES=" + startWebservices);

        if (singleAgentId != null && !singleAgentId.isEmpty()) {
            commandLine.add("-e");
            commandLine.add("LANSGSTREAM_TESTER_AGENTID=" + singleAgentId);
        }

        commandLine.add("-v");
        commandLine.add(appDirectory.getAbsolutePath() + ":/code/application");
        commandLine.add("-v");
        commandLine.add(tmpInstanceFile.getAbsolutePath() + ":/code/instance.yaml");
        commandLine.add("-v");
        commandLine.add(tmpSecretsFile.getAbsolutePath() + ":/code/secrets.yaml");
        commandLine.add("--add-host");
        commandLine.add("minio.minio-dev.svc.cluster.local:127.0.0.1");
        commandLine.add("--add-host");
        commandLine.add("my-cluster-kafka-bootstrap.kafka:127.0.0.1");
        // gateway
        commandLine.add("-p");
        commandLine.add("8091:8091");
        // web service
        commandLine.add("-p");
        commandLine.add("8090:8090");

        if (memory != null && !memory.isEmpty()) {
            commandLine.add("--memory");
            commandLine.add(memory);
        }

        if (cpus != null && !cpus.isEmpty()) {
            commandLine.add("--cpus");
            commandLine.add(cpus);
        }

        if (dockerAdditionalArgs != null) {
            commandLine.addAll(dockerAdditionalArgs);
        }

        commandLine.add(imageName);

        if (getRootCmd().isVerbose()) {
            System.out.println("Executing:");
            System.out.println(String.join(" ", commandLine));
        }

        ProcessBuilder processBuilder = new ProcessBuilder(commandLine).inheritIO();
        Process process = processBuilder.start();
        process.waitFor();
    }
}
