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

import ai.langstream.admin.client.AdminClient;
import ai.langstream.admin.client.AdminClientConfiguration;
import ai.langstream.admin.client.http.GenericRetryExecution;
import ai.langstream.admin.client.http.HttpClientProperties;
import ai.langstream.admin.client.http.NoRetryPolicy;
import ai.langstream.cli.api.model.Gateways;
import ai.langstream.cli.commands.VersionProvider;
import ai.langstream.cli.commands.applications.MermaidAppDiagramGenerator;
import ai.langstream.cli.commands.applications.UIAppCmd;
import ai.langstream.cli.util.LocalFileReferenceResolver;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import lombok.SneakyThrows;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;
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
            description = "Instance file path")
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
            names = {"--start-database"},
            description = "Start the embedded HerdDB database")
    private boolean startDatabase = true;

    @CommandLine.Option(
            names = {"--start-webservices"},
            description = "Start LangStream webservices")
    private boolean startWebservices = true;

    @CommandLine.Option(
            names = {"--start-ui"},
            description = "Start the UI")
    private boolean startUI = true;

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

    @CommandLine.Option(
            names = {"--langstream-runtime-version"},
            description = "Version of the LangStream runtime to use")
    private String dockerImageVersion = VersionProvider.getMavenVersion();

    @CommandLine.Option(
            names = {"--langstream-runtime-docker-image"},
            description = "Docker image of the LangStream runtime to use")
    private String dockerImageName;

    @CommandLine.Option(
            names = {"--dry-run"},
            description =
                    "Dry-run mode. Do not deploy the application but only resolves placeholders and display the result.")
    private boolean dryRun;

    @Override
    @SneakyThrows
    public void run() {

        if (dockerImageVersion != null && dockerImageVersion.endsWith("-SNAPSHOT")) {
            // built-from-sources, not a release
            dockerImageVersion = "latest-dev";
        }

        if (dockerImageName == null) {
            if (dockerImageVersion != null && dockerImageVersion.equals("latest-dev")) {
                // built-from-sources, not a release
                dockerImageName = "langstream/langstream-runtime-tester";
            } else {
                // default to latest
                dockerImageName = "ghcr.io/langstream/langstream-runtime-tester";
            }
        }
        startBroker = !dryRun && startBroker;
        startDatabase = !dryRun && startDatabase;
        startS3 = !dryRun && startS3;
        startWebservices = !dryRun && startWebservices;

        final File appDirectory = checkFileExistsOrDownload(appPath);
        final File instanceFile;

        if (instanceFilePath != null) {
            instanceFile = checkFileExistsOrDownload(instanceFilePath);
        } else {
            instanceFile = null;
        }
        final File secretsFile = checkFileExistsOrDownload(secretFilePath);

        log("Tenant " + getTenant());
        log("Application " + name);
        log("Application directory: " + appDirectory.getAbsolutePath());
        if (singleAgentId != null && !singleAgentId.isEmpty()) {
            log("Filter agent: " + singleAgentId);
        } else {
            log("Running all the agents in the application");
        }
        log("Instance file: " + instanceFile);
        if (secretsFile != null) {
            log("Secrets file: " + secretsFile.getAbsolutePath());
        }
        log("Start broker: " + startBroker);
        log("Start S3: " + startS3);
        log("Start Database: " + startDatabase);
        log("Start Webservices " + startWebservices);

        if (appDirectory == null) {
            throw new IllegalArgumentException("application files are required");
        }

        downloadDependencies(appDirectory.toPath(), getClient(), this::log);

        final String secretsContents;
        final String instanceContents;

        if (instanceFile != null) {
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
        } else {
            if (startBroker || dryRun) {
                instanceContents =
                        "instance:\n"
                                + "  streamingCluster:\n"
                                + "    type: \"kafka\"\n"
                                + "    configuration:\n"
                                + "      admin:\n"
                                + "        bootstrap.servers: localhost:9092";
                log(
                        "Using default instance file that connects to the Kafka broker inside the docker container");
            } else {
                throw new IllegalArgumentException(
                        "instance file is required if broker is not started");
            }
        }

        if (secretsFile != null) {
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
        } else {
            secretsContents = null;
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
                startWebservices,
                startDatabase,
                dryRun);
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
            boolean startWebservices,
            boolean startDatabase,
            boolean dryRun)
            throws Exception {
        File tmpInstanceFile = Files.createTempFile("instance", ".yaml").toFile();
        Files.write(tmpInstanceFile.toPath(), instanceContents.getBytes(StandardCharsets.UTF_8));
        File tmpSecretsFile = null;
        if (secretsContents != null) {
            tmpSecretsFile = Files.createTempFile("secrets", ".yaml").toFile();
            Files.write(tmpSecretsFile.toPath(), secretsContents.getBytes(StandardCharsets.UTF_8));
        }
        String imageName = dockerImageName + ":" + dockerImageVersion;
        List<String> commandLine = new ArrayList<>();
        commandLine.add(dockerCommand);
        commandLine.add("run");
        commandLine.add("--rm");
        commandLine.add("-i");
        commandLine.add("-e");
        commandLine.add("START_BROKER=" + startBroker);
        commandLine.add("-e");
        commandLine.add("START_MINIO=" + startS3);
        commandLine.add("-e");
        commandLine.add("START_HERDDB=" + startDatabase);
        commandLine.add("-e");
        commandLine.add("LANSGSTREAM_TESTER_TENANT=" + tenant);
        commandLine.add("-e");
        commandLine.add("LANSGSTREAM_TESTER_APPLICATIONID=" + applicationId);
        commandLine.add("-e");
        commandLine.add("LANSGSTREAM_TESTER_STARTWEBSERVICES=" + startWebservices);
        commandLine.add("-e");
        commandLine.add("LANSGSTREAM_TESTER_DRYRUN=" + dryRun);

        if (singleAgentId != null && !singleAgentId.isEmpty()) {
            commandLine.add("-e");
            commandLine.add("LANSGSTREAM_TESTER_AGENTID=" + singleAgentId);
        }

        commandLine.add("-v");
        commandLine.add(appDirectory.getAbsolutePath() + ":/code/application");
        commandLine.add("-v");
        commandLine.add(tmpInstanceFile.getAbsolutePath() + ":/code/instance.yaml");
        if (tmpSecretsFile != null) {
            commandLine.add("-v");
            commandLine.add(tmpSecretsFile.getAbsolutePath() + ":/code/secrets.yaml");
        }

        // with this tricks K8S examples work on Docker seamlessly
        commandLine.add("--add-host");
        commandLine.add("minio.minio-dev.svc.cluster.local:127.0.0.1");
        commandLine.add("--add-host");
        commandLine.add("herddb.herddb-dev.svc.cluster.local:127.0.0.1");
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

        final Path outputLog = Files.createTempFile("langstream", ".log");
        log("Logging to file: " + outputLog.toAbsolutePath());
        ProcessBuilder processBuilder =
                new ProcessBuilder(commandLine)
                        .redirectErrorStream(true)
                        .redirectOutput(outputLog.toFile());
        Process process = processBuilder.start();
        Executors.newSingleThreadExecutor().execute(() -> tailLogSysOut(outputLog));

        if (startUI) {
            Executors.newSingleThreadExecutor()
                    .execute(() -> startUI(tenant, applicationId, outputLog, process));
        }
        process.waitFor();
    }

    private void startUI(String tenant, String applicationId, Path outputLog, Process process) {
        String body;
        try (final AdminClient localAdminClient =
                new AdminClient(
                        AdminClientConfiguration.builder()
                                .tenant(tenant)
                                .webServiceUrl("http://localhost:8090")
                                .build(),
                        getAdminClientLogger(),
                        HttpClientProperties.builder()
                                .retry(() -> new GenericRetryExecution(NoRetryPolicy.INSTANCE))
                                .build()); ) {

            while (true) {
                if (!process.isAlive()) {
                    return;
                }
                try {
                    body = localAdminClient.applications().get(applicationId, false);
                    break;
                } catch (Throwable e) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
        final List<Gateways.Gateway> gateways = Gateways.readFromApplicationDescription(body);
        final String finalBody = body;
        final String mermaidDefinition = MermaidAppDiagramGenerator.generate(finalBody);
        UIAppCmd.startServer(
                () -> {
                    final UIAppCmd.AppModel appModel = new UIAppCmd.AppModel();
                    appModel.setTenant(tenant);
                    appModel.setApplicationId(applicationId);
                    appModel.setRemoteBaseUrl("ws://localhost:8091");
                    appModel.setGateways(gateways);
                    appModel.setApplicationDefinition(finalBody);
                    appModel.setMermaidDefinition(mermaidDefinition);
                    return appModel;
                },
                "ws://localhost:8091",
                getTailLogSupplier(outputLog));
    }

    private UIAppCmd.LogSupplier getTailLogSupplier(Path outputLog) {
        return line -> {
            while (true) {
                try (Tailer tailer =
                        Tailer.builder()
                                .setTailFromEnd(true)
                                .setStartThread(false)
                                .setTailerListener(
                                        new TailerListener() {
                                            @Override
                                            public void fileNotFound() {}

                                            @Override
                                            public void fileRotated() {}

                                            @Override
                                            public void handle(Exception e) {}

                                            @Override
                                            public void handle(String s) {
                                                line.accept(s);
                                            }

                                            @Override
                                            public void init(Tailer tailer) {}
                                        })
                                .setDelayDuration(Duration.ofMillis(100))
                                .setFile(outputLog.toFile())
                                .get(); ) {
                    tailer.run();
                }
            }
        };
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
