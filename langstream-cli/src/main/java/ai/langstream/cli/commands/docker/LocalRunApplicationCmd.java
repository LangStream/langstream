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
import ai.langstream.cli.LangStreamCLI;
import ai.langstream.cli.NamedProfile;
import ai.langstream.cli.api.model.Gateways;
import ai.langstream.cli.commands.applications.MermaidAppDiagramGenerator;
import ai.langstream.cli.commands.applications.UIAppCmd;
import ai.langstream.cli.util.DockerImageUtils;
import ai.langstream.cli.util.LocalFileReferenceResolver;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.WatchService;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;
import org.apache.commons.lang3.SystemUtils;
import picocli.CommandLine;

@CommandLine.Command(name = "run", header = "Run the application on a docker container")
public class LocalRunApplicationCmd extends BaseDockerCmd {

    protected static final String LOCAL_DOCKER_RUN_PROFILE = "local-docker-run";

    private static final Set<Path> temporaryFiles = ConcurrentHashMap.newKeySet();
    private static final AtomicReference<ProcessHandle> dockerProcess = new AtomicReference<>();

    @CommandLine.Parameters(description = "ID of the application")
    private String applicationId;

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
            description = "Start webservices")
    private boolean startWebservices = true;

    @CommandLine.Option(
            names = {"--start-ui"},
            description = "Start the UI")
    private boolean startUI = true;

    @CommandLine.Option(
            names = {"--watch-files"},
            description = "Watch files and apply the changes automatically (only Python files)")
    private boolean watchFiles = true;

    @CommandLine.Option(
            names = {"-up", "--ui-port"},
            description = "Port for the local webserver and UI. If 0, a random port will be used.",
            defaultValue = "8092")
    private int uiPort = 8092;

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
            description = "Version of the runtime to use",
            defaultValue = "${env:LANGSTREAM_RUNTIME_DOCKER_IMAGE_VERSION}")
    private String dockerImageVersion;

    @CommandLine.Option(
            names = {"--langstream-runtime-docker-image"},
            description = "Docker image of the runtime to use",
            defaultValue = "${env:LANGSTREAM_RUNTIME_DOCKER_IMAGE}")
    private String dockerImageName;

    @CommandLine.Option(
            names = {"--dry-run"},
            description =
                    "Dry-run mode. Do not deploy the application but only resolves placeholders and display the result.")
    private boolean dryRun;

    @CommandLine.Option(
            names = {"--tenant"},
            description = "Tenant name. Default to local-docker-run")
    private String tenant = "default";

    @Override
    @SneakyThrows
    public void run() {

        DockerImageUtils.DockerImage dockerImage =
                DockerImageUtils.computeDockerImage(dockerImageVersion, dockerImageName);

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

        log("Tenant: " + tenant);
        log("Application: " + applicationId);
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
        log("Using docker image: " + dockerImage.getFullName());

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
                instanceContents = "instance:\n" + "  streamingCluster:\n" + "    type: \"noop\"\n";
                log("The broker is disabled, you won't be able to use topics");
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

        updateLocalDockerRunProfile(tenant);

        Runtime.getRuntime().addShutdownHook(new Thread(this::cleanEnvironment));

        executeOnDocker(
                tenant,
                applicationId,
                singleAgentId,
                appDirectory,
                instanceContents,
                secretsContents,
                startBroker,
                startS3,
                startWebservices,
                startDatabase,
                dryRun,
                dockerImage);
    }

    private void cleanEnvironment() {
        if (dockerProcess.get() != null) {
            dockerProcess.get().destroyForcibly();
        }
        if (temporaryFiles.isEmpty()) {
            return;
        }
        log("Cleaning environment");
        for (Path temporaryFile : temporaryFiles) {
            debug("Deleting temporary file: " + temporaryFile);
            FileUtils.deleteQuietly(temporaryFile.toFile());
        }
    }

    private void updateLocalDockerRunProfile(String tenant) {
        updateConfig(
                langStreamCLIConfig -> {
                    final NamedProfile profile = new NamedProfile();
                    profile.setName(LOCAL_DOCKER_RUN_PROFILE);
                    profile.setTenant(tenant);
                    profile.setWebServiceUrl("http://localhost:8090");
                    profile.setApiGatewayUrl("ws://localhost:8091");
                    langStreamCLIConfig.updateProfile(LOCAL_DOCKER_RUN_PROFILE, profile);
                    log("profile " + LOCAL_DOCKER_RUN_PROFILE + " updated");
                });
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
            boolean dryRun,
            DockerImageUtils.DockerImage dockerImage)
            throws Exception {
        final File appTmp = prepareAppDirectory(appDirectory);
        File tmpInstanceFile = prepareInstanceFile(instanceContents);
        File tmpSecretsFile = prepareSecretsFile(secretsContents);
        String imageName = dockerImage.getFullName();
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
        commandLine.add(appTmp.getAbsolutePath() + ":/code/application");
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

        // agent control
        commandLine.add("-p");
        commandLine.add("8790:8790");

        // python services (LangServe on uvicorn on port 8000)
        commandLine.add("-p");
        commandLine.add("8000:8000");

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

        try (WatchService watcher = FileSystems.getDefault().newWatchService(); ) {
            if (watchFiles) {
                Path python = appTmp.toPath().resolve("python");
                if (Files.isDirectory(python)) { // only watch if python is present
                    startWatchService(appTmp.toPath(), watcher);
                } else {
                    log(
                            "Python directory "
                                    + python.toAbsolutePath()
                                    + " not found, not watching files");
                }
            }

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

            if (startUI) {
                Executors.newSingleThreadExecutor()
                        .execute(() -> startUI(tenant, applicationId, outputLog, process));
            }

            final int exited = process.waitFor();
            // wait for the log to be printed
            Thread.sleep(1000);
            if (exited != 0) {
                throw new RuntimeException("Process exited with code " + exited);
            }
        }
    }

    private void startWatchService(Path applicationDirectory, WatchService watcher)
            throws Exception {

        ApplicationWatcher.watchApplication(
                applicationDirectory,
                file -> {
                    if (file.endsWith("/python") || file.endsWith(".py")) {
                        log("A python file has changed, restarting the application");
                        restartAgents();
                    } else {
                        log("A file has changed: " + file);
                    }
                },
                watcher);
    }

    private void restartAgents() {
        HttpURLConnection urlConnection = null;
        try {
            URL url = new URL("http://localhost:8790/commands/restart");
            urlConnection = (HttpURLConnection) url.openConnection();
            urlConnection.setRequestMethod("POST");
            urlConnection.getContent();
        } catch (Exception e) {
            log("Could not reload the agents: " + e);
        } finally {
            if (urlConnection != null) {
                urlConnection.disconnect();
            }
        }
    }

    private File prepareSecretsFile(String secretsContents) throws IOException {
        File tmpSecretsFile = null;
        if (secretsContents != null) {
            tmpSecretsFile = createReadableTempFile("secrets", secretsContents);
        }
        return tmpSecretsFile;
    }

    private File prepareInstanceFile(String instanceContents) throws IOException {
        return createReadableTempFile("instance", instanceContents);
    }

    private File prepareAppDirectory(File appDirectory) throws IOException {
        // depending on the docker engine, we should ensure the permissions are properly set.
        // On MacOs, Docker runs on a VM, so the user mapping between the host and the container is
        // performed by the engine.
        // On Linux, we need to make sure all the mounted volumes are readable from others because
        // the docker container runs with a different user than the file owner.
        if (SystemUtils.IS_OS_MAC) {
            return appDirectory;
        }
        final File appTmp = generateTempFile("app");
        FileUtils.copyDirectory(appDirectory, appTmp);
        makeDirOrFileReadable(appTmp);
        return appTmp;
    }

    private static File createReadableTempFile(String prefix, String instanceContents)
            throws IOException {
        File tempFile = generateTempFile(prefix);
        Files.write(tempFile.toPath(), instanceContents.getBytes(StandardCharsets.UTF_8));
        makeDirOrFileReadable(tempFile);
        return tempFile;
    }

    private static void makeDirOrFileReadable(File file) throws IOException {
        if (file.isDirectory()) {
            for (File child : file.listFiles()) {
                makeDirOrFileReadable(child);
            }
            Files.setPosixFilePermissions(
                    file.toPath(), PosixFilePermissions.fromString("rwxr-xr-x"));
        } else {
            Files.setPosixFilePermissions(
                    file.toPath(), PosixFilePermissions.fromString("rw-r--r--"));
        }
    }

    @SneakyThrows
    private static File generateTempFile(String prefix) {
        Path home = LangStreamCLI.getLangstreamCLIHomeDirectory();
        final String generatedName = ".langstream_" + prefix + "_" + System.nanoTime();
        if (home == null) {
            home = new File(".").toPath();
        } else {
            home = home.resolve("tmp");
            Files.createDirectories(home);
        }
        File tempFile = home.resolve(generatedName).toFile();
        temporaryFiles.add(tempFile.toPath());
        return tempFile;
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
                uiPort,
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
                getTailLogSupplier(outputLog),
                getLogger());
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
