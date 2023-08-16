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
package com.datastax.oss.sga.tests;

import com.dajudge.kindcontainer.K3sContainer;
import com.dajudge.kindcontainer.K3sContainerVersion;
import com.dajudge.kindcontainer.KubernetesImageSpec;
import com.dajudge.kindcontainer.helm.Helm3Container;
import com.datastax.oss.sga.deployer.k8s.api.crds.agents.AgentCustomResource;
import com.datastax.oss.sga.deployer.k8s.api.crds.apps.ApplicationCustomResource;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolume;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.rbac.ClusterRole;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.LocalPortForward;
import io.fabric8.kubernetes.client.dsl.ContainerResource;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.BindMode;

@Slf4j
public class BaseEndToEndTest implements TestWatcher {


    public static final File TEST_LOGS_DIR = new File("target", "e2e-test-logs");
    protected static final String TENANT_NAMESPACE_PREFIX = "sga-tenant-";
    protected static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());

    interface KubeServer {
        void start();

        void ensureImage(String image);

        void stop();

        String getKubeConfig();
    }

    static class RunningHostCluster implements PythonFunctionIT.KubeServer {
        @Override
        public void start() {
        }

        @Override
        public void ensureImage(String image) {
        }

        @Override
        public void stop() {
            try (final KubernetesClient client = new KubernetesClientBuilder()
                    .withConfig(Config.fromKubeconfig(kubeServer.getKubeConfig()))
                    .build();) {
                client.namespaces().withName(namespace).delete();
            }
        }

        @Override
        @SneakyThrows
        public String getKubeConfig() {
            final String kubeConfig = Config.getKubeconfigFilename();
            return Files.readString(Paths.get(kubeConfig), StandardCharsets.UTF_8);
        }
    }

    static class LocalK3sContainer implements PythonFunctionIT.KubeServer {

        K3sContainer container;
        final Path basePath = Paths.get("/tmp", "sga-tests-image");

        @Override
        public void start() {
            container =
                    new K3sContainer(new KubernetesImageSpec<>(K3sContainerVersion.VERSION_1_25_0)
                            .withImage("rancher/k3s:v1.25.3-k3s1"));
            container.withFileSystemBind(basePath.toFile().getAbsolutePath(), "/images", BindMode.READ_WRITE);
            // container.withNetwork(network);
            container.start();

        }

        @Override
        public void ensureImage(String image) {
            loadImage(basePath, image);
        }

        @SneakyThrows
        private void loadImage(Path basePath, String image) {
            final String id = DockerClientFactory.lazyClient().inspectImageCmd(image).exec()
                    .getId().replace("sha256:", "");

            final Path hostPath = basePath.resolve(id);
            if (!hostPath.toFile().exists()) {
                log.info("Saving image {} locally", image);
                final InputStream in = DockerClientFactory.lazyClient()
                        .saveImageCmd(image)
                        .exec();

                try (final OutputStream outputStream = Files.newOutputStream(hostPath);) {
                    in.transferTo(outputStream);
                } catch (Exception ex) {
                    hostPath.toFile().delete();
                    throw ex;
                }
            }

            log.info("Loading image {} in the k3s container", image);
            if (container.execInContainer("ctr", "images", "import", "/images/" + id)
                    .getExitCode() != 0) {
                throw new RuntimeException("Failed to load image " + image);
            }
        }

        @Override
        public void stop() {
            if (container != null) {
                container.stop();
            }
        }

        @Override
        public String getKubeConfig() {
            return container.getKubeconfig();
        }
    }


    protected static KubeServer kubeServer;
    protected static Helm3Container helm3Container;
    protected static KubernetesClient client;
    protected static String controlPlaneBaseUrl;
    protected static String apiGatewayBaseUrl;
    protected static String namespace;

    @Override
    public void testAborted(ExtensionContext context, Throwable cause) {
        testFailed(context, cause);
    }

    @Override
    public void testFailed(ExtensionContext context, Throwable cause) {
        log.error("Test {} failed", context.getDisplayName(), cause);
        final String prefix = "%s.%s".formatted(context.getTestClass().get().getSimpleName(),
                context.getTestMethod().get().getName());
        dumpAllPodsLogs(prefix, namespace);
        dumpEvents(prefix);
        dumpAllResources(prefix);

    }

    protected static void applyManifest(String manifest) {
        client.load(new ByteArrayInputStream(manifest.getBytes(StandardCharsets.UTF_8)))
                .inNamespace(namespace)
                .serverSideApply();
    }
    protected static void applyManifestNoNamespace(String manifest) {
        client.load(new ByteArrayInputStream(manifest.getBytes(StandardCharsets.UTF_8)))
                .serverSideApply();
    }

    protected void executeCliCommand(String... args)
            throws InterruptedException, IOException {

        String[] allArgs = new String[args.length + 1];
        allArgs[0] = "bin/sga-cli";
        System.arraycopy(args, 0, allArgs, 1, args.length);
        runProcess(allArgs);
    }

    protected void executeCliCommandUntilOutput(Predicate<String> predicate, String... args)
            throws InterruptedException, IOException {

        String[] allArgs = new String[args.length + 1];
        allArgs[0] = "bin/sga-cli";
        System.arraycopy(args, 0, allArgs, 1, args.length);
        runProcessUntilOutput(allArgs, predicate);
    }

    private static void runProcess(String[] allArgs) throws InterruptedException, IOException {
        runProcess(allArgs, false);
    }

    private static void runProcess(String[] allArgs, boolean allowFailures) throws InterruptedException, IOException {
        ProcessBuilder processBuilder = new ProcessBuilder(allArgs)
                .directory(Paths.get("..").toFile())
                .inheritIO()
                .redirectOutput(ProcessBuilder.Redirect.INHERIT)
                .redirectError(ProcessBuilder.Redirect.INHERIT);
        final int exitCode = processBuilder.start().waitFor();
        if (exitCode != 0 && !allowFailures) {
            throw new RuntimeException("Command failed with code: " + exitCode + " args: " + Arrays.toString(allArgs));
        }
    }


    private static void runProcessUntilOutput(String[] allArgs, Predicate<String> lineTester) throws InterruptedException, IOException {

        final Path output = Files.createTempFile("output", "txt");
        final Path error = Files.createTempFile("error", "txt");

        ProcessBuilder processBuilder = new ProcessBuilder(allArgs)
                .directory(Paths.get("..").toFile())
                .redirectOutput(output.toFile())
                .redirectError(error.toFile());

        final BufferedReader br = new BufferedReader(new FileReader(output.toFile()));
        final Process process = processBuilder.start();
        try {

            Awaitility.await().until(() -> {
                String line;
                while ((line = br.readLine()) != null) {
                    if (lineTester.test(line)) {
                        return true;
                    }
                }
                return false;
            });
        } finally {
            process.destroyForcibly();
        }
    }

    @AfterAll
    @SneakyThrows
    public static void destroy() {
        if (client != null) {
            client.close();
        }
        if (helm3Container != null) {
            helm3Container.close();
        }
        if (kubeServer != null) {
            kubeServer.stop();
        }
    }


    @BeforeAll
    @SneakyThrows
    public static void setup() {
        namespace = "sga-test-" + UUID.randomUUID().toString().substring(0, 8);

        // kubeServer = new LocalK3sContainer();
        kubeServer = new RunningHostCluster();
        kubeServer.start();

        client = new KubernetesClientBuilder()
                .withConfig(Config.fromKubeconfig(kubeServer.getKubeConfig()))
                .build();

        // cleanup previous runs
        client.namespaces().withLabel("app", "sga-test")
                        .delete();
        client.namespaces()
                .list()
                .getItems()
                .stream().filter(ns -> ns.getMetadata().getName().startsWith(TENANT_NAMESPACE_PREFIX)).forEach(ns -> {
                    client.namespaces().withName(ns.getMetadata().getName()).delete();
                });

        client.resource(new NamespaceBuilder()
                        .withNewMetadata()
                        .withName(namespace)
                        .withLabels(Map.of("app", "sga-test"))
                        .endMetadata()
                        .build())
                .serverSideApply();


        final Path tempFile = Files.createTempFile("sga-test-kube", ".yaml");
        Files.write(tempFile,
                kubeServer.getKubeConfig().getBytes(StandardCharsets.UTF_8));
        System.out.println("To inspect the container\nKUBECONFIG=" + tempFile.toFile().getAbsolutePath() + " k9s");

        final CompletableFuture<Void> kafkaFuture = CompletableFuture.runAsync(() -> installKafka());
        final CompletableFuture<Void> minioFuture = CompletableFuture.runAsync(() -> installMinio());
        List<CompletableFuture<Void>> imagesFutures = new ArrayList<>();

        imagesFutures.add(CompletableFuture.runAsync(() ->
                kubeServer.ensureImage("datastax/sga-control-plane:latest-dev")));
        imagesFutures.add(CompletableFuture.runAsync(() ->
                kubeServer.ensureImage( "datastax/sga-deployer:latest-dev")));
        imagesFutures.add(CompletableFuture.runAsync(() ->
                kubeServer.ensureImage("datastax/sga-runtime:latest-dev")));
        imagesFutures.add(CompletableFuture.runAsync(() ->
                kubeServer.ensureImage("datastax/sga-api-gateway:latest-dev")));

        final CompletableFuture<Void> sgaFuture =
                CompletableFuture.runAsync(() -> installSga());
        CompletableFuture.allOf(kafkaFuture, minioFuture, imagesFutures.get(0), imagesFutures.get(1), imagesFutures.get(2),
                sgaFuture).join();
        awaitControlPlaneReady();
        awaitApiGatewayReady();
    }

    @SneakyThrows
    private static void installSga() {
        final String hostPath = Paths.get("..", "helm", "sga").toFile().getAbsolutePath();

        client.resources(ClusterRole.class)
                        .withName("sga-deployer")
                        .delete();
        client.resources(ClusterRole.class)
                .withName("sga-control-plane")
                .delete();
        client.resources(ClusterRole.class)
                .withName("sga-api-gateway")
                .delete();
        client.resources(ClusterRole.class)
                .withName("sga-client")
                .delete();

        client.resources(ClusterRoleBinding.class)
                .withName("sga-control-plane-role-binding")
                .delete();
        client.resources(ClusterRoleBinding.class)
                .withName("sga-deployer-role-binding")
                .delete();
        client.resources(ClusterRoleBinding.class)
                .withName("sga-api-gateway-role-binding")
                .delete();
        client.resources(ClusterRoleBinding.class)
                .withName("sga-client-role-binding")
                .delete();


        log.info("installing sga with helm, using chart from {}", hostPath);
        final String deleteCmd =
                "helm delete %s -n %s".formatted("sga", namespace);
        log.info("Running {}", deleteCmd);
        runProcess(deleteCmd.split(" "), true);

        final String values = """
                controlPlane:
                  resources:
                    requests:
                      cpu: 256m
                      memory: 512Mi
                  app:
                    config:
                      application.storage.apps.type: kubernetes
                      application.storage.apps.configuration.namespaceprefix: %s
                      application.storage.apps.configuration.deployer-runtime.image: datastax/sga-runtime:latest-dev
                      application.storage.apps.configuration.deployer-runtime.image-pull-policy: Never
                      application.storage.global.type: kubernetes
                      application.storage.code.type: s3
                      application.storage.code.configuration.endpoint: http://minio.minio-dev.svc.cluster.local:9000
                      application.storage.code.configuration.access-key: minioadmin
                      application.storage.code.configuration.secret-key: minioadmin
                                
                deployer:
                  replicaCount: 1
                  resources:
                    requests:
                      cpu: 256m
                      memory: 512Mi
                  app:
                    config:
                      agentResources:
                        cpuPerUnit: 0.1
                        memPerUnit: 128
                      clusterRuntime:
                          kubernetes:
                            namespace-prefix: %s
                      codeStorage:
                        type: s3
                        endpoint: http://minio.minio-dev.svc.cluster.local:9000
                        access-key: minioadmin
                        secret-key: minioadmin
                                
                apiGateway:
                  podAnnotations: {}
                  podSecurityContext: {}
                  securityContext: {}
                  app:
                    config:
                      application.storage.apps.configuration.namespaceprefix: %s
                """.formatted(TENANT_NAMESPACE_PREFIX, TENANT_NAMESPACE_PREFIX, TENANT_NAMESPACE_PREFIX);
        final Path tempFile = Files.createTempFile("sga-test", ".yaml");
        Files.write(tempFile, values.getBytes(StandardCharsets.UTF_8));


        final String cmd =
                "helm install --debug --timeout 360s %s -n %s %s --values %s".formatted(
                        "sga", namespace, hostPath, tempFile.toFile().getAbsolutePath());
        log.info("Running {}", cmd);
        runProcess(cmd.split(" "));
        log.info("Helm install completed");

    }

    private static void awaitControlPlaneReady() {
        log.info("waiting for control plane to be ready");

        client.apps().deployments().inNamespace(namespace).withName("sga-control-plane")
                .waitUntilCondition(
                        d -> d.getStatus().getReadyReplicas() != null && d.getStatus().getReadyReplicas() == 1, 120,
                        TimeUnit.SECONDS);
        log.info("control plane ready, port forwarding");

        final String podName = client.pods().inNamespace(namespace).withLabel("app.kubernetes.io/name", "sga-control-plane")
                .list()
                .getItems()
                .get(0)
                .getMetadata().getName();
        final int webServicePort = nextFreePort();
        client.pods().inNamespace(namespace)
                .withName(podName)
                .portForward(8090, webServicePort);
        controlPlaneBaseUrl = "http://localhost:" + webServicePort;
    }

    private static void awaitApiGatewayReady() {
        log.info("waiting for api gateway to be ready");

        client.apps().deployments().inNamespace(namespace).withName("sga-api-gateway")
                .waitUntilCondition(
                        d -> d.getStatus().getReadyReplicas() != null && d.getStatus().getReadyReplicas() == 1, 120,
                        TimeUnit.SECONDS);
        log.info("api gateway ready, port forwarding");

        final String podName = client.pods().inNamespace(namespace).withLabel("app.kubernetes.io/name", "sga-api-gateway")
                .list()
                .getItems()
                .get(0)
                .getMetadata().getName();
        final int port = nextFreePort();
        final LocalPortForward localPortForward = client.pods().inNamespace(namespace)
                .withName(podName)
                .portForward(8091, port);
        log.info("api gateway port forwarding started {}", localPortForward.getLocalAddress());
        apiGatewayBaseUrl = "ws://localhost:" + localPortForward.getLocalPort();
    }

    @SneakyThrows
    private static void installKafka() {
        log.info("installing kafka");
        client.resource(new NamespaceBuilder()
                .withNewMetadata()
                .withName("kafka")
                .endMetadata()
                .build())
                        .serverSideApply();
        runProcess("kubectl apply -f https://strimzi.io/install/latest?namespace=kafka -n kafka".split(" "));
        runProcess("kubectl apply -f https://strimzi.io/examples/latest/kafka/kafka-persistent-single.yaml -n kafka".split(" "));
        log.info("waiting kafka to be ready");
        runProcess("kubectl wait kafka/my-cluster --for=condition=Ready --timeout=300s -n kafka".split(" "));
        log.info("kafka installed");
    }




    private static synchronized int nextFreePort() {
        int exceptionCount = 0;
        while (true) {
            try (ServerSocket ss = new ServerSocket(0)) {
                return ss.getLocalPort();
            } catch (Exception e) {
                exceptionCount++;
                if (exceptionCount > 100) {
                    throw new RuntimeException("Unable to allocate socket port", e);
                }
            }
        }
    }


    static void installMinio() {
        applyManifestNoNamespace("""
                # Deploys a new Namespace for the MinIO Pod
                apiVersion: v1
                kind: Namespace
                metadata:
                  name: minio-dev # Change this value if you want a different namespace name
                  labels:
                    name: minio-dev # Change this value to match metadata.name
                ---
                # Deploys a new MinIO Pod into the metadata.namespace Kubernetes namespace
                #
                # The `spec.containers[0].args` contains the command run on the pod
                # The `/data` directory corresponds to the `spec.containers[0].volumeMounts[0].mountPath`
                # That mount path corresponds to a Kubernetes HostPath which binds `/data` to a local drive or volume on the worker node where the pod runs
                #\s
                apiVersion: v1
                kind: Pod
                metadata:
                  labels:
                    app: minio
                  name: minio
                  namespace: minio-dev # Change this value to match the namespace metadata.name
                spec:
                  containers:
                  - name: minio
                    image: quay.io/minio/minio:latest
                    command:
                    - /bin/bash
                    - -c
                    args:\s
                    - minio server /data --console-address :9090
                    volumeMounts:
                    - mountPath: /data
                      name: localvolume # Corresponds to the `spec.volumes` Persistent Volume
                    ports:
                      -  containerPort: 9090
                         protocol: TCP
                         name: console
                      -  containerPort: 9000
                         protocol: TCP
                         name: s3
                  volumes:
                  - name: localvolume
                    hostPath: # MinIO generally recommends using locally-attached volumes
                      path: /mnt/disk1/data # Specify a path to a local drive or volume on the Kubernetes worker node
                      type: DirectoryOrCreate # The path to the last directory must exist
                ---
                apiVersion: v1
                kind: Service
                metadata:
                  labels:
                    app: minio
                  name: minio
                  namespace: minio-dev # Change this value to match the namespace metadata.name
                spec:
                  ports:
                    - port: 9090
                      protocol: TCP
                      targetPort: 9090
                      name: console
                    - port: 9000
                      protocol: TCP
                      targetPort: 9000
                      name: s3
                  selector:
                    app: minio
                """);
    }

    protected void withPodLogs(String podName, String namespace, int tailingLines, BiConsumer<String, String> consumer) {
        if (podName != null) {
            try {
                client.pods().inNamespace(namespace)
                        .withName(podName)
                        .get().getSpec().getContainers().forEach(container -> {
                            final ContainerResource containerResource = client.pods().inNamespace(namespace)
                                    .withName(podName)
                                    .inContainer(container.getName());
                            if (tailingLines > 0) {
                                containerResource.tailingLines(tailingLines);
                            }
                            final String containerLog = containerResource.getLog();
                            consumer.accept(container.getName(), containerLog);
                        });
            } catch (Throwable t) {
                log.error("failed to get pod {} logs: {}", podName, t.getMessage());
            }
        }
    }


    protected void dumpAllPodsLogs(String filePrefix, String namespace) {
        client.pods().inNamespace(namespace).list().getItems()
                .forEach(pod -> dumpPodLogs(pod.getMetadata().getName(), namespace, filePrefix));
    }

    protected void dumpPodLogs(String podName, String namespace, String filePrefix) {
        TEST_LOGS_DIR.mkdirs();
        withPodLogs(podName, namespace, -1, (container, logs) -> {
            final File outputFile = new File(TEST_LOGS_DIR, "%s.%s.%s.log".formatted(filePrefix, podName, container));
            try (FileWriter writer = new FileWriter(outputFile)) {
                writer.write(logs);
            } catch (IOException e) {
                log.error("failed to write pod {} logs to file {}", podName, outputFile, e);
            }
        });
    }

    protected void dumpAllResources(String filePrefix) {
        dumpResources(filePrefix, Pod.class);
        dumpResources(filePrefix, StatefulSet.class);
        dumpResources(filePrefix, Job.class);
        dumpResources(filePrefix, AgentCustomResource.class);
        dumpResources(filePrefix, ApplicationCustomResource.class);
    }

    private void dumpResources(String filePrefix, Class<? extends HasMetadata> clazz) {
        client.resources(clazz)
                .inNamespace(namespace)
                .list()
                .getItems()
                .forEach(resource -> dumpResource(filePrefix, resource));
    }

    protected void dumpResource(String filePrefix, HasMetadata resource) {
        TEST_LOGS_DIR.mkdirs();
        final File outputFile = new File(TEST_LOGS_DIR,
                "%s-%s-%s.log".formatted(filePrefix, resource.getKind(), resource.getMetadata().getName()));
        try (FileWriter writer = new FileWriter(outputFile)) {
            writer.write(MAPPER.writeValueAsString(resource));
        } catch (Throwable e) {
            log.error("failed to write resource to file {}", outputFile, e);
        }
    }

    protected void dumpEvents(String filePrefix) {
        TEST_LOGS_DIR.mkdirs();
        final File outputFile = new File(TEST_LOGS_DIR, "%s-events.log".formatted(filePrefix));
        try (FileWriter writer = new FileWriter(outputFile)) {
            client.resources(Event.class).inAnyNamespace().list()
                    .getItems()
                    .forEach(event -> {
                        try {
                            writer.write("[%s] [%s/%s] %s: %s\n".formatted(event.getMetadata().getNamespace(),
                                    event.getInvolvedObject().getKind(),
                                    event.getInvolvedObject().getName(), event.getReason(), event.getMessage()));
                        } catch (IOException e) {
                            log.error("failed to write event {} to file {}", event, outputFile, e);
                        }
                    });
        } catch (Throwable e) {
            log.error("failed to write events logs to file {}", outputFile, e);
        }
    }


}
