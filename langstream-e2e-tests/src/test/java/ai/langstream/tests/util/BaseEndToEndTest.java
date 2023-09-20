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
package ai.langstream.tests.util;

import ai.langstream.api.model.StreamingCluster;
import ai.langstream.deployer.k8s.api.crds.agents.AgentCustomResource;
import ai.langstream.deployer.k8s.api.crds.apps.ApplicationCustomResource;
import ai.langstream.impl.parser.ModelBuilder;
import ai.langstream.tests.util.k8s.LocalK3sContainer;
import ai.langstream.tests.util.k8s.RunningHostCluster;
import ai.langstream.tests.util.kafka.LocalRedPandaClusterProvider;
import ai.langstream.tests.util.kafka.RemoteKafkaProvider;
import com.dajudge.kindcontainer.helm.Helm3Container;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Status;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.rbac.ClusterRole;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.dsl.ContainerResource;
import io.fabric8.kubernetes.client.dsl.ExecListener;
import io.fabric8.kubernetes.client.dsl.ExecWatch;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;

@Slf4j
public class BaseEndToEndTest implements TestWatcher {

    public static final String TOPICS_PREFIX = "ls-test-";

    public static final String CATEGORY_NEEDS_CREDENTIALS = "needs-credentials";

    private static final String LANGSTREAM_TAG =
            System.getProperty("langstream.tests.tag", "latest-dev");

    private static final String LANGSTREAM_K8S = System.getProperty("langstream.tests.k8s", "host");
    private static final String LANGSTREAM_STREAMING =
            System.getProperty("langstream.tests.streaming", "local-redpanda");

    public static final File TEST_LOGS_DIR = new File("target", "e2e-test-logs");
    protected static final String TENANT_NAMESPACE_PREFIX = "ls-tenant-";
    protected static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    protected static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());
    protected static KubeCluster kubeCluster;
    protected static StreamingClusterProvider streamingClusterProvider;
    protected static File instanceFile;
    protected static Helm3Container helm3Container;
    protected static KubernetesClient client;
    protected static String namespace;
    protected static AtomicBoolean initialized = new AtomicBoolean(false);

    @Override
    public void testAborted(ExtensionContext context, Throwable cause) {
        testFailed(context, cause);
    }

    @Override
    public void testFailed(ExtensionContext context, Throwable cause) {
        log.error("Test {} failed", context.getDisplayName(), cause);
        final String prefix =
                "%s.%s"
                        .formatted(
                                context.getTestClass().orElseThrow().getSimpleName(),
                                context.getTestMethod().orElseThrow().getName());
        dumpTest(prefix);
    }

    private static void dumpTest(String prefix) {
        dumpAllPodsLogs(prefix);
        dumpEvents(prefix);
        dumpAllResources(prefix);
        dumpProcessOutput(prefix, "kubectl-nodes", "kubectl describe nodes".split(" "));
    }

    protected static void applyManifest(String manifest, String namespace) {
        client.load(new ByteArrayInputStream(manifest.getBytes(StandardCharsets.UTF_8)))
                .inNamespace(namespace)
                .serverSideApply();
    }

    protected static void deleteManifest(String manifest, String namespace) {
        client.load(new ByteArrayInputStream(manifest.getBytes(StandardCharsets.UTF_8)))
                .inNamespace(namespace)
                .delete();
    }

    protected static void applyManifestNoNamespace(String manifest) {
        client.load(new ByteArrayInputStream(manifest.getBytes(StandardCharsets.UTF_8)))
                .serverSideApply();
    }

    @SneakyThrows
    protected static void copyFileToClientContainer(File file, String toPath) {
        final String podName =
                client.pods()
                        .inNamespace(namespace)
                        .withLabel("app.kubernetes.io/name", "langstream-client")
                        .list()
                        .getItems()
                        .get(0)
                        .getMetadata()
                        .getName();
        if (file.isFile()) {
            runProcess(
                    "kubectl cp %s %s:%s -n %s"
                            .formatted(file.getAbsolutePath(), podName, toPath, namespace)
                            .split(" "));
        } else {
            runProcess(
                    "kubectl cp %s %s:%s -n %s"
                            .formatted(file.getAbsolutePath(), podName, toPath, namespace)
                            .split(" "));
        }
    }

    @SneakyThrows
    protected static String executeCommandOnClient(String... args) {
        return executeCommandOnClient(2, TimeUnit.MINUTES, args);
    }

    @SneakyThrows
    protected static String executeCommandOnClient(long timeout, TimeUnit unit, String... args) {
        final Pod pod =
                client.pods()
                        .inNamespace(namespace)
                        .withLabel("app.kubernetes.io/name", "langstream-client")
                        .list()
                        .getItems()
                        .get(0);
        return execInPod(
                        pod.getMetadata().getName(),
                        pod.getSpec().getContainers().get(0).getName(),
                        args)
                .get(timeout, unit);
    }

    @SneakyThrows
    protected static ConsumeGatewayMessage consumeOneMessageFromGateway(
            String applicationId, String gatewayId, String... extraArgs) {
        final String command =
                "bin/langstream gateway consume %s %s %s -n 1"
                        .formatted(applicationId, gatewayId, String.join(" ", extraArgs));
        final String response = executeCommandOnClient(command);
        final List<String> lines = response.lines().collect(Collectors.toList());
        if (lines.size() <= 1) {
            return null;
        }
        final String secondLine = lines.get(1);
        return ConsumeGatewayMessage.readValue(secondLine);
    }

    public static void runProcess(String[] allArgs) throws InterruptedException, IOException {
        runProcess(allArgs, false);
    }

    public static void runProcess(String[] allArgs, boolean allowFailures)
            throws InterruptedException, IOException {
        ProcessBuilder processBuilder =
                new ProcessBuilder(allArgs)
                        .directory(Paths.get("..").toFile())
                        .inheritIO()
                        .redirectOutput(ProcessBuilder.Redirect.INHERIT)
                        .redirectError(ProcessBuilder.Redirect.INHERIT);
        final int exitCode = processBuilder.start().waitFor();
        if (exitCode != 0 && !allowFailures) {
            throw new RuntimeException(
                    "Command failed with code: " + exitCode + " args: " + Arrays.toString(allArgs));
        }
    }

    public static CompletableFuture<String> execInPod(
            String podName, String containerName, String... cmds) {
        return execInPodInNamespace(namespace, podName, containerName, cmds);
    }

    public static CompletableFuture<String> execInPodInNamespace(
            String namespace, String podName, String containerName, String... cmds) {

        final String cmd = String.join(" ", cmds);
        log.info(
                "Executing in pod {}: {}",
                containerName == null ? podName : podName + "/" + containerName,
                cmd);
        final AtomicBoolean completed = new AtomicBoolean(false);
        final CompletableFuture<String> response = new CompletableFuture<>();
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final ByteArrayOutputStream error = new ByteArrayOutputStream();

        final ExecListener listener =
                new ExecListener() {
                    @Override
                    public void onOpen() {}

                    @Override
                    public void onFailure(Throwable t, Response failureResponse) {
                        if (!completed.compareAndSet(false, true)) {
                            return;
                        }
                        log.warn(
                                "Error executing {} encountered; \nstderr: {}\nstdout: {}",
                                cmd,
                                error.toString(StandardCharsets.UTF_8),
                                out.toString(),
                                t);
                        response.completeExceptionally(t);
                    }

                    @Override
                    public void onExit(int code, Status status) {
                        if (!completed.compareAndSet(false, true)) {
                            return;
                        }
                        if (code != 0) {
                            log.warn(
                                    "Error executing {} encountered; \ncode: {}\n stderr: {}\nstdout: {}",
                                    cmd,
                                    code,
                                    error.toString(StandardCharsets.UTF_8),
                                    out.toString(StandardCharsets.UTF_8));
                            response.completeExceptionally(
                                    new RuntimeException(
                                            "Command failed with err code: "
                                                    + code
                                                    + ", stderr: "
                                                    + error.toString(StandardCharsets.UTF_8)));
                        } else {
                            log.info(
                                    "Command completed {}; \nstderr: {}\nstdout: {}",
                                    cmd,
                                    error.toString(StandardCharsets.UTF_8),
                                    out.toString(StandardCharsets.UTF_8));
                            response.complete(out.toString(StandardCharsets.UTF_8));
                        }
                    }

                    @Override
                    public void onClose(int rc, String reason) {
                        if (!completed.compareAndSet(false, true)) {
                            return;
                        }
                        log.info(
                                "Command completed {}; \nstderr: {}\nstdout: {}",
                                cmd,
                                error.toString(StandardCharsets.UTF_8),
                                out.toString(StandardCharsets.UTF_8));
                        response.complete(out.toString(StandardCharsets.UTF_8));
                    }
                };

        ExecWatch exec = null;

        try {
            exec =
                    client.pods()
                            .inNamespace(namespace)
                            .withName(podName)
                            .inContainer(containerName)
                            .writingOutput(out)
                            .writingError(error)
                            .usingListener(listener)
                            .exec("bash", "-c", cmd);
        } catch (Throwable t) {
            log.error("Execution failed for {}", cmd, t);
            completed.set(true);
            response.completeExceptionally(t);
        }

        final ExecWatch execToClose = exec;
        response.whenComplete(
                (s, ex) -> {
                    closeQuietly(execToClose);
                    closeQuietly(out);
                    closeQuietly(error);
                });

        return response;
    }

    public static void closeQuietly(Closeable c) {
        if (c != null) {
            try {
                c.close();
            } catch (IOException e) {
                log.error("error while closing {}: {}", c, e);
            }
        }
    }

    @AfterAll
    @SneakyThrows
    public static void destroy() {
        cleanupAllEndToEndTestsNamespaces();
        if (streamingClusterProvider != null) {
            streamingClusterProvider.stop();
        }
        if (client != null) {
            client.close();
        }
        if (helm3Container != null) {
            helm3Container.close();
        }
        if (kubeCluster != null) {
            kubeCluster.stop();
        }
    }

    @BeforeAll
    @SneakyThrows
    public static void setup() {
        if (initialized.get()) {
            log.info("Skip setup since it's already done");
            return;
        }

        kubeCluster = getKubeCluster();
        kubeCluster.start();

        client =
                new KubernetesClientBuilder()
                        .withConfig(Config.fromKubeconfig(kubeCluster.getKubeConfig()))
                        .build();

        streamingClusterProvider = getStreamingClusterProvider();

        try {

            final Path tempFile = Files.createTempFile("ls-test-kube", ".yaml");
            Files.writeString(tempFile, kubeCluster.getKubeConfig());
            System.out.println(
                    "To inspect the container\nKUBECONFIG="
                            + tempFile.toFile().getAbsolutePath()
                            + " k9s");

            final CompletableFuture<StreamingCluster> streamingClusterFuture =
                    CompletableFuture.supplyAsync(() -> streamingClusterProvider.start());
            final CompletableFuture<Void> minioFuture =
                    CompletableFuture.runAsync(BaseEndToEndTest::installMinio);
            List<CompletableFuture<Void>> imagesFutures = new ArrayList<>();

            imagesFutures.add(
                    CompletableFuture.runAsync(
                            () ->
                                    kubeCluster.ensureImage(
                                            "langstream/langstream-control-plane:latest-dev")));
            imagesFutures.add(
                    CompletableFuture.runAsync(
                            () ->
                                    kubeCluster.ensureImage(
                                            "langstream/langstream-deployer:latest-dev")));
            imagesFutures.add(
                    CompletableFuture.runAsync(
                            () ->
                                    kubeCluster.ensureImage(
                                            "langstream/langstream-runtime:latest-dev")));
            imagesFutures.add(
                    CompletableFuture.runAsync(
                            () ->
                                    kubeCluster.ensureImage(
                                            "langstream/langstream-api-gateway:latest-dev")));

            CompletableFuture.allOf(
                            minioFuture,
                            imagesFutures.get(0),
                            imagesFutures.get(1),
                            imagesFutures.get(2),
                            imagesFutures.get(3))
                    .join();

            final StreamingCluster streamingCluster = streamingClusterFuture.join();

            final Map<String, Map<String, Object>> instanceContent =
                    Map.of(
                            "instance",
                            Map.of(
                                    "streamingCluster",
                                    streamingCluster,
                                    "computeCluster",
                                    Map.of("type", "kubernetes")));

            instanceFile = Files.createTempFile("ls-test", ".yaml").toFile();
            YAML_MAPPER.writeValue(instanceFile, instanceContent);
            initialized.set(true);

        } catch (Throwable ee) {
            dumpTest("BeforeAll");
            throw ee;
        }
    }

    private static StreamingClusterProvider getStreamingClusterProvider() {
        switch (LANGSTREAM_STREAMING) {
            case "local-redpanda":
                return new LocalRedPandaClusterProvider(client);
            case "remote-kafka":
                return new RemoteKafkaProvider();
            default:
                throw new IllegalArgumentException(
                        "Unknown LANGSTREAM_STREAMING: " + LANGSTREAM_STREAMING);
        }
    }

    private static KubeCluster getKubeCluster() {
        switch (LANGSTREAM_K8S) {
            case "k3s":
                return new LocalK3sContainer();
            case "host":
                return new RunningHostCluster();
            default:
                throw new IllegalArgumentException("Unknown LANGSTREAM_K8S: " + LANGSTREAM_K8S);
        }
    }

    @BeforeEach
    @SneakyThrows
    public void setupSingleTest() {
        // cleanup previous runs
        cleanupAllEndToEndTestsNamespaces();
        namespace = "ls-test-" + UUID.randomUUID().toString().substring(0, 8);

        client.resource(
                        new NamespaceBuilder()
                                .withNewMetadata()
                                .withName(namespace)
                                .withLabels(Map.of("app", "ls-test"))
                                .endMetadata()
                                .build())
                .serverSideApply();
    }

    @AfterEach
    public void cleanupAfterEach() {
        cleanupAllEndToEndTestsNamespaces();
        streamingClusterProvider.cleanup();
    }

    private static void cleanupAllEndToEndTestsNamespaces() {
        client.namespaces().withLabel("app", "ls-test").delete();
        client.namespaces().list().getItems().stream()
                .map(ns -> ns.getMetadata().getName())
                .filter(ns -> ns.startsWith(TENANT_NAMESPACE_PREFIX))
                .forEach(ns -> deleteTenantNamespace(ns));
    }

    private static void deleteTenantNamespace(String ns) {
        try {
            final List<AgentCustomResource> agents =
                    client.resources(AgentCustomResource.class).inNamespace(ns).list().getItems();
            for (AgentCustomResource agent : agents) {
                agent.getMetadata().setFinalizers(List.of());
                client.resource(agent).inNamespace(ns).serverSideApply();
                client.resources(AgentCustomResource.class)
                        .inNamespace(ns)
                        .withName(agent.getMetadata().getNamespace())
                        .delete();
            }
            final List<ApplicationCustomResource> apps =
                    client.resources(ApplicationCustomResource.class)
                            .inNamespace(ns)
                            .list()
                            .getItems();

            for (ApplicationCustomResource app : apps) {
                app.getMetadata().setFinalizers(List.of());
                client.resource(app).inNamespace(ns).serverSideApply();
                client.resources(ApplicationCustomResource.class)
                        .inNamespace(ns)
                        .withName(app.getMetadata().getNamespace())
                        .delete();
            }
        } catch (Throwable tt) {
            log.error("Error deleting tenant namespace: " + ns, tt);
        }
        client.namespaces().withName(ns).delete();
    }

    @SneakyThrows
    protected void installLangStreamCluster(boolean authentication) {
        CompletableFuture.runAsync(() -> installLangStream(authentication)).get();
        awaitControlPlaneReady();
        awaitApiGatewayReady();
    }

    @SneakyThrows
    private static void installLangStream(boolean authentication) {
        client.resources(ClusterRole.class).withName("langstream-deployer").delete();
        client.resources(ClusterRole.class).withName("langstream-control-plane").delete();
        client.resources(ClusterRole.class).withName("langstream-api-gateway").delete();
        client.resources(ClusterRole.class).withName("langstream-client").delete();

        client.resources(ClusterRoleBinding.class)
                .withName("langstream-control-plane-role-binding")
                .delete();
        client.resources(ClusterRoleBinding.class)
                .withName("langstream-deployer-role-binding")
                .delete();
        client.resources(ClusterRoleBinding.class)
                .withName("langstream-api-gateway-role-binding")
                .delete();
        client.resources(ClusterRoleBinding.class)
                .withName("langstream-client-role-binding")
                .delete();

        final String baseImageRepository =
                LANGSTREAM_TAG.equals("latest-dev") ? "langstream" : "ghcr.io/langstream";
        final String imagePullPolicy =
                LANGSTREAM_TAG.equals("latest-dev") ? "Never" : "IfNotPresent";
        final String values =
                """
                        images:
                            tag: %s
                        controlPlane:
                          image:
                            repository: %s/langstream-control-plane
                            pullPolicy: %s
                          resources:
                            requests:
                              cpu: 0.2
                              memory: 256Mi
                          app:
                            config:
                              application.storage.global.type: kubernetes
                              application.security.enabled: false

                        deployer:
                          image:
                            repository: %s/langstream-deployer
                            pullPolicy: %s
                          replicaCount: 1
                          resources:
                            requests:
                              cpu: 0.1
                              memory: 256Mi
                          app:
                            config:
                              agentResources:
                                cpuPerUnit: 0.2
                                memPerUnit: 128
                        client:
                          image:
                            repository: %s/langstream-cli
                            pullPolicy: %s
                          resources:
                            requests:
                              cpu: 0.1
                              memory: 256Mi

                        apiGateway:
                          image:
                            repository: %s/langstream-api-gateway
                            pullPolicy: %s
                          resources:
                            requests:
                              cpu: 0.2
                              memory: 256Mi
                          app:
                            config:
                             logging.level.org.apache.tomcat.websocket: debug

                        runtime:
                            image: %s/langstream-runtime
                            imagePullPolicy: %s
                        tenants:
                            defaultTenant:
                                create: false
                            namespacePrefix: %s
                        codeStorage:
                          type: s3
                          configuration:
                            endpoint: http://minio.minio-dev.svc.cluster.local:9000
                            access-key: minioadmin
                            secret-key: minioadmin
                        """
                        .formatted(
                                LANGSTREAM_TAG,
                                baseImageRepository,
                                imagePullPolicy,
                                baseImageRepository,
                                imagePullPolicy,
                                baseImageRepository,
                                imagePullPolicy,
                                baseImageRepository,
                                imagePullPolicy,
                                baseImageRepository,
                                imagePullPolicy,
                                TENANT_NAMESPACE_PREFIX);
        final Path tempFile = Files.createTempFile("langstream-test", ".yaml");
        Files.writeString(tempFile, values);

        runProcess(
                "helm repo add langstream https://langstream.github.io/charts/".split(" "), true);
        final String cmd =
                "helm install --debug --timeout 360s %s langstream/langstream -n %s --values %s"
                        .formatted("langstream", namespace, tempFile.toFile().getAbsolutePath());
        log.info("Running {}", cmd);
        runProcess(cmd.split(" "));
        log.info("Helm install completed");
    }

    private static void awaitControlPlaneReady() {
        log.info("waiting for control plane to be ready");

        client.apps()
                .deployments()
                .inNamespace(namespace)
                .withName("langstream-control-plane")
                .waitUntilCondition(
                        d ->
                                d.getStatus().getReadyReplicas() != null
                                        && d.getStatus().getReadyReplicas() == 1,
                        120,
                        TimeUnit.SECONDS);
        log.info("control plane ready");
    }

    @SneakyThrows
    private static void awaitApiGatewayReady() {
        log.info("waiting for api gateway to be ready");

        client.apps()
                .deployments()
                .inNamespace(namespace)
                .withName("langstream-api-gateway")
                .waitUntilCondition(
                        d ->
                                d.getStatus().getReadyReplicas() != null
                                        && d.getStatus().getReadyReplicas() == 1,
                        120,
                        TimeUnit.SECONDS);
        log.info("api gateway ready");
    }

    static void installMinio() {
        applyManifestNoNamespace(
                """
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
                            resources:
                              requests:
                                cpu: 50m
                                memory: 512Mi
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

    protected static void withPodLogs(
            String podName,
            String namespace,
            int tailingLines,
            BiConsumer<String, String> consumer) {
        if (podName != null) {
            try {
                final PodSpec podSpec =
                        client.pods().inNamespace(namespace).withName(podName).get().getSpec();
                List<Container> all = new ArrayList<>();
                final List<Container> containers = podSpec.getContainers();
                all.addAll(containers);
                final List<Container> init = podSpec.getInitContainers();
                if (init != null) {
                    all.addAll(init);
                }
                for (Container container : all) {
                    try {
                        final ContainerResource containerResource =
                                client.pods()
                                        .inNamespace(namespace)
                                        .withName(podName)
                                        .inContainer(container.getName());
                        if (tailingLines > 0) {
                            containerResource.tailingLines(tailingLines);
                        }
                        final String containerLog = containerResource.getLog();
                        consumer.accept(container.getName(), containerLog);
                    } catch (Throwable t) {
                        log.error(
                                "failed to get pod {} container {} logs: {}",
                                podName,
                                container.getName(),
                                t.getMessage());
                    }
                }
            } catch (Throwable t) {
                log.error("failed to get pod {} logs: {}", podName, t.getMessage());
            }
        }
    }

    protected static void dumpAllPodsLogs(String filePrefix) {
        getAllUserNamespaces().forEach(ns -> dumpAllPodsLogs(filePrefix, ns));
    }

    protected static void dumpAllPodsLogs(String filePrefix, String namespace) {
        client.pods()
                .inNamespace(namespace)
                .list()
                .getItems()
                .forEach(pod -> dumpPodLogs(pod.getMetadata().getName(), namespace, filePrefix));
    }

    protected static void dumpPodLogs(String podName, String namespace, String filePrefix) {
        TEST_LOGS_DIR.mkdirs();
        withPodLogs(
                podName,
                namespace,
                -1,
                (container, logs) -> {
                    final File outputFile =
                            new File(
                                    TEST_LOGS_DIR,
                                    "%s.%s.%s.log".formatted(filePrefix, podName, container));
                    try (FileWriter writer = new FileWriter(outputFile)) {
                        writer.write(logs);
                    } catch (IOException e) {
                        log.error("failed to write pod {} logs to file {}", podName, outputFile, e);
                    }
                });
    }

    protected static void dumpAllResources(String filePrefix) {
        final List<String> namespaces = getAllUserNamespaces();
        dumpResources(filePrefix, Pod.class, namespaces);
        dumpResources(filePrefix, StatefulSet.class, namespaces);
        dumpResources(filePrefix, Job.class, namespaces);
        dumpResources(filePrefix, AgentCustomResource.class, namespaces);
        dumpResources(filePrefix, ApplicationCustomResource.class, namespaces);
        dumpResources(filePrefix, Node.class, namespaces);
    }

    private static List<String> getAllUserNamespaces() {
        return client.namespaces().list().getItems().stream()
                .map(n -> n.getMetadata().getName())
                .filter(n -> !n.equals("kube-system"))
                .collect(Collectors.toList());
    }

    private static void dumpResources(
            String filePrefix, Class<? extends HasMetadata> clazz, List<String> namespaces) {
        try {
            for (String namespace : namespaces) {
                client.resources(clazz)
                        .inNamespace(namespace)
                        .list()
                        .getItems()
                        .forEach(resource -> dumpResource(filePrefix, resource));
            }
        } catch (Throwable t) {
            log.warn(
                    "failed to dump resources of type {}: {}",
                    clazz.getSimpleName(),
                    t.getMessage());
        }
    }

    protected static void dumpResource(String filePrefix, HasMetadata resource) {
        TEST_LOGS_DIR.mkdirs();
        final File outputFile =
                new File(
                        TEST_LOGS_DIR,
                        "%s-%s-%s.txt"
                                .formatted(
                                        filePrefix,
                                        resource.getKind(),
                                        resource.getMetadata().getName()));
        try (FileWriter writer = new FileWriter(outputFile)) {
            writer.write(YAML_MAPPER.writeValueAsString(resource));
        } catch (Throwable e) {
            log.error("failed to write resource to file {}", outputFile, e);
        }
    }

    protected static void dumpProcessOutput(String filePrefix, String filename, String... args) {
        final File outputFile =
                new File(TEST_LOGS_DIR, "%s-%s-stdout.txt".formatted(filePrefix, filename));
        final File outputFileErr =
                new File(TEST_LOGS_DIR, "%s-%s-stderr.txt".formatted(filePrefix, filename));

        try {
            ProcessBuilder processBuilder =
                    new ProcessBuilder(args)
                            .directory(Paths.get("..").toFile())
                            .redirectOutput(ProcessBuilder.Redirect.to(outputFile))
                            .redirectError(ProcessBuilder.Redirect.to(outputFileErr));
            processBuilder.start().waitFor();
        } catch (Throwable e) {
            log.error("failed to write process output to file {}", outputFile, e);
        }
    }

    protected static void dumpEvents(String filePrefix) {
        TEST_LOGS_DIR.mkdirs();
        final File outputFile = new File(TEST_LOGS_DIR, "%s-events.txt".formatted(filePrefix));
        try (FileWriter writer = new FileWriter(outputFile)) {
            client.resources(Event.class)
                    .inAnyNamespace()
                    .list()
                    .getItems()
                    .forEach(
                            event -> {
                                try {

                                    writer.write(
                                            "[%s] [%s/%s] %s: %s\n"
                                                    .formatted(
                                                            event.getMetadata().getNamespace(),
                                                            event.getInvolvedObject().getKind(),
                                                            event.getInvolvedObject().getName(),
                                                            event.getReason(),
                                                            event.getMessage()));
                                } catch (IOException e) {
                                    log.error(
                                            "failed to write event {} to file {}",
                                            event,
                                            outputFile,
                                            e);
                                }
                            });
        } catch (Throwable e) {
            log.error("failed to write events logs to file {}", outputFile, e);
        }
    }

    @SneakyThrows
    protected static List<String> getAllTopics() {
        return streamingClusterProvider.getTopics();
    }

    protected static void setupTenant(String tenant) {
        executeCommandOnClient(
                """
                        bin/langstream tenants put %s &&
                        bin/langstream configure tenant %s"""
                        .formatted(tenant, tenant)
                        .replace(System.lineSeparator(), " ")
                        .split(" "));
    }

    protected static void deployLocalApplication(String applicationId, String appDir) {
        deployLocalApplication(applicationId, appDir, null);
    }

    protected static void awaitApplicationReady(
            String applicationId, int expectedRunningTotalExecutors) {
        Awaitility.await()
                .atMost(3, TimeUnit.MINUTES)
                .pollInterval(5, TimeUnit.SECONDS)
                .until(() -> isApplicationReady(applicationId, expectedRunningTotalExecutors));
    }

    protected static boolean isApplicationReady(
            String applicationId, int expectedRunningTotalExecutors) {
        final String response =
                executeCommandOnClient(
                        "bin/langstream apps get %s".formatted(applicationId).split(" "));
        final List<String> lines = response.lines().collect(Collectors.toList());
        final String appLine = lines.get(1);
        final List<String> lineAsList =
                Arrays.stream(appLine.split(" "))
                        .filter(s -> !s.isBlank())
                        .collect(Collectors.toList());
        System.out.println("app line " + lineAsList);
        final String status = lineAsList.get(3);
        if (status != null && status.equals("ERROR_DEPLOYING")) {
            log.info("application {} is in ERROR_DEPLOYING state, dumping status", applicationId);
            executeCommandOnClient(
                    "bin/langstream apps get %s -o yaml".formatted(applicationId).split(" "));
            throw new IllegalStateException("application is in ERROR_DEPLOYING state");
        }
        if (lineAsList.size() <= 5) {
            return false;
        }
        final String replicasReady = lineAsList.get(5);
        System.out.println("replicasReady " + replicasReady);
        return replicasReady.equals(
                expectedRunningTotalExecutors + "/" + expectedRunningTotalExecutors);
    }

    @SneakyThrows
    protected static void deployLocalApplication(
            String applicationId, String appDirName, Map<String, String> env) {
        String testAppsBaseDir = "src/test/resources/apps";
        String testSecretBaseDir = "src/test/resources/secrets";

        final File appDir = Paths.get(testAppsBaseDir, appDirName).toFile();
        final File secretFile = Paths.get(testSecretBaseDir, "secret1.yaml").toFile();
        validateApp(appDir, secretFile);
        copyFileToClientContainer(appDir, "/tmp/app");
        copyFileToClientContainer(instanceFile, "/tmp/instance.yaml");

        copyFileToClientContainer(secretFile, "/tmp/secrets.yaml");

        String beforeCmd = "";
        if (env != null) {
            beforeCmd =
                    env.entrySet().stream()
                            .map(e -> "export %s=%s".formatted(e.getKey(), e.getValue()))
                            .collect(Collectors.joining(" && "));
            beforeCmd += " && ";
        }

        executeCommandOnClient(
                (beforeCmd
                                + "bin/langstream apps deploy %s -app /tmp/app -i /tmp/instance.yaml -s /tmp/secrets.yaml")
                        .formatted(applicationId)
                        .split(" "));
    }

    private static void validateApp(File appDir, File secretFile) throws Exception {
        final ModelBuilder.ApplicationWithPackageInfo model =
                ModelBuilder.buildApplicationInstance(
                        List.of(appDir.toPath()),
                        Files.readString(instanceFile.toPath()),
                        Files.readString(secretFile.toPath()));
        model.getApplication()
                .getModules()
                .values()
                .forEach(
                        m -> {
                            m.getTopics()
                                    .keySet()
                                    .forEach(
                                            t -> {
                                                if (!t.startsWith(TOPICS_PREFIX)) {
                                                    throw new IllegalStateException(
                                                            "All topics must start with "
                                                                    + TOPICS_PREFIX
                                                                    + ". Found "
                                                                    + t);
                                                }
                                            });
                        });
    }

    protected static Map<String, String> getAppEnvMapFromSystem(List<String> names) {
        final Map<String, String> result = new HashMap<>();
        for (String name : names) {
            result.put(name, getAppEnvFromSystem(name));
        }
        return result;
    }

    protected static String getAppEnvFromSystem(String name) {

        final String fromSystemProperty = System.getProperty("langstream.tests.app.env." + name);
        if (fromSystemProperty != null) {
            return null;
        }
        final String fromEnv = System.getenv("LANGSTREAM_TESTS_APP_ENV_" + name);
        if (fromEnv != null) {
            return fromEnv;
        }

        final String fromEnvDirect = System.getenv(name);
        if (fromEnvDirect != null) {
            return fromEnvDirect;
        }
        throw new IllegalStateException(
                ("failed to get app env variable %s from system or env. Possible env variables: %s, "
                                + "LANGSTREAM_TESTS_APP_ENV_%s. Possible system properties: langstream.tests.app.env.%s")
                        .formatted(name, name, name, name));
    }
}
