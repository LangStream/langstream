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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import ai.langstream.api.model.StreamingCluster;
import ai.langstream.deployer.k8s.api.crds.agents.AgentCustomResource;
import ai.langstream.deployer.k8s.api.crds.apps.ApplicationCustomResource;
import ai.langstream.impl.parser.ModelBuilder;
import ai.langstream.tests.util.codestorage.LocalMinioCodeStorageProvider;
import ai.langstream.tests.util.codestorage.RemoteCodeStorageProvider;
import ai.langstream.tests.util.k8s.LocalK3sContainer;
import ai.langstream.tests.util.k8s.RunningHostCluster;
import ai.langstream.tests.util.kafka.LocalRedPandaClusterProvider;
import ai.langstream.tests.util.kafka.RemoteKafkaProvider;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Status;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.rbac.ClusterRole;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.dsl.ExecListener;
import io.fabric8.kubernetes.client.dsl.ExecWatch;
import io.fabric8.kubernetes.client.dsl.Loggable;
import io.fabric8.kubernetes.client.readiness.Readiness;
import io.jsonwebtoken.Jwts;
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
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;

@Slf4j
public class BaseEndToEndTest implements TestWatcher {

    public static final String TOPICS_PREFIX = "ls-test-";

    private static final boolean LANGSTREAM_RECYCLE_ENV =
            Boolean.parseBoolean(System.getProperty("langstream.tests.recycleenv", "false"));

    private static final String LANGSTREAM_REPO = System.getProperty("langstream.tests.repository");

    private static final String LANGSTREAM_TAG =
            System.getProperty("langstream.tests.tag", "latest-dev");

    private static final String LANGSTREAM_K8S =
            SystemOrEnv.getProperty("LANGSTREAM_TESTS_K8S", "langstream.tests.k8s", "host");
    private static final String LANGSTREAM_STREAMING =
            SystemOrEnv.getProperty(
                    "LANGSTREAM_TESTS_STREAMING", "langstream.tests.streaming", "local-redpanda");
    private static final String LANGSTREAM_CODESTORAGE =
            SystemOrEnv.getProperty(
                    "LANGSTREAM_TESTS_CODESTORAGE", "langstream.tests.codestorage", "local-minio");

    private static final String LANGSTREAM_APPS_RESOURCES_CPU =
            SystemOrEnv.getProperty(
                    "LANGSTREAM_TESTS_APPS_RESOURCES_CPU",
                    "langstream.tests.apps.resources.cpu",
                    "0.4");
    private static final String LANGSTREAM_APPS_RESOURCES_MEM =
            SystemOrEnv.getProperty(
                    "LANGSTREAM_TESTS_APPS_RESOURCES_MEM",
                    "langstream.tests.apps.resources.mem",
                    "256");

    public static final File TEST_LOGS_DIR = new File("target", "e2e-test-logs");
    protected static final String TENANT_NAMESPACE_PREFIX = "ls-tenant-";
    protected static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    protected static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());
    protected static KubeCluster kubeCluster;
    protected static StreamingClusterProvider streamingClusterProvider;
    protected static StreamingCluster streamingCluster;
    protected static File instanceFile;
    protected static CodeStorageProvider codeStorageProvider;
    protected static CodeStorageProvider.CodeStorageConfig codeStorageConfig;
    protected static KubernetesClient client;
    protected static String namespace;
    protected static KeyPair controlPlaneAuthKeyPair;

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

    public static KubernetesClient getClient() {
        return client;
    }

    private static void dumpTest(String prefix) {
        dumpAllPodsLogs(prefix + ".logs");
        dumpEvents(prefix);
        dumpAllResources(prefix + ".resource");
        dumpProcessOutput(prefix, "kubectl-nodes", "kubectl describe nodes".split(" "));
    }

    public static void applyManifest(String manifest, String namespace) {
        client.load(new ByteArrayInputStream(manifest.getBytes(StandardCharsets.UTF_8)))
                .inNamespace(namespace)
                .serverSideApply();
    }

    protected static void deleteManifest(String manifest, String namespace) {
        client.load(new ByteArrayInputStream(manifest.getBytes(StandardCharsets.UTF_8)))
                .inNamespace(namespace)
                .delete();
    }

    public static void applyManifestNoNamespace(String manifest) {
        client.load(new ByteArrayInputStream(manifest.getBytes(StandardCharsets.UTF_8)))
                .serverSideApply();
    }

    @SneakyThrows
    public static void copyFileToClientContainer(File file, String toPath) {
        final String podName =
                getFirstPodFromDeployment("langstream-client").getMetadata().getName();
        copyFileToPod(podName, namespace, file, toPath);
    }

    @SneakyThrows
    public static void copyFileToPod(String podName, String namespace, File file, String toPath) {
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

    private static Pod getFirstPodFromDeployment(String deploymentName) {
        final List<Pod> items =
                client.pods()
                        .inNamespace(namespace)
                        .withLabel("app.kubernetes.io/name", deploymentName)
                        .list()
                        .getItems();
        if (items.isEmpty()) {
            return null;
        }
        return items.get(0);
    }

    @SneakyThrows
    public static String executeCommandOnClient(String... args) {
        return executeCommandOnClient(2, TimeUnit.MINUTES, args);
    }

    @SneakyThrows
    protected static String executeCommandOnClient(long timeout, TimeUnit unit, String... args) {
        final Pod pod = getFirstPodFromDeployment("langstream-client");
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
        if (!LANGSTREAM_RECYCLE_ENV) {
            if (streamingClusterProvider != null) {
                streamingClusterProvider.stop();
                streamingClusterProvider = null;
            }
            if (codeStorageProvider != null) {
                codeStorageProvider.stop();
                codeStorageProvider = null;
            }
            if (client != null) {
                client.close();
                client = null;
            }
            if (kubeCluster != null) {
                kubeCluster.stop();
                kubeCluster = null;
            }
        }
    }

    @BeforeAll
    @SneakyThrows
    public static void setup() {
        if (kubeCluster == null) {
            kubeCluster = getKubeCluster();
            kubeCluster.start();
        }

        if (client == null) {
            client =
                    new KubernetesClientBuilder()
                            .withConfig(Config.fromKubeconfig(kubeCluster.getKubeConfig()))
                            .build();
        }

        if (streamingClusterProvider == null) {
            streamingClusterProvider = getStreamingClusterProvider();
        }

        if (codeStorageProvider == null) {
            codeStorageProvider = getCodeStorageProvider();
        }

        try {

            final Path tempFile = Files.createTempFile("ls-test-kube", ".yaml");
            Files.writeString(tempFile, kubeCluster.getKubeConfig());
            System.out.println(
                    "To inspect the container\nKUBECONFIG="
                            + tempFile.toFile().getAbsolutePath()
                            + " k9s");

            final CompletableFuture<StreamingCluster> streamingClusterFuture =
                    CompletableFuture.supplyAsync(() -> streamingClusterProvider.start());
            final CompletableFuture<CodeStorageProvider.CodeStorageConfig> minioFuture =
                    CompletableFuture.supplyAsync(() -> codeStorageProvider.start());
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
                            imagesFutures.get(0),
                            imagesFutures.get(1),
                            imagesFutures.get(2),
                            imagesFutures.get(3))
                    .join();

            streamingCluster = streamingClusterFuture.join();

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

            codeStorageConfig = minioFuture.join();

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

    private static CodeStorageProvider getCodeStorageProvider() {
        switch (LANGSTREAM_CODESTORAGE) {
            case "local-minio":
                return new LocalMinioCodeStorageProvider();
            case "remote":
                return new RemoteCodeStorageProvider();
            default:
                throw new IllegalArgumentException(
                        "Unknown LANGSTREAM_CODESTORAGE: " + LANGSTREAM_CODESTORAGE);
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
        cleanupEnv();

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

    private void cleanupEnv() {
        if (codeStorageProvider != null) {
            codeStorageProvider.cleanup();
        }
        if (streamingClusterProvider != null) {
            streamingClusterProvider.cleanup();
        }
    }

    @AfterEach
    public void cleanupAfterEach() {
        // do not cleanup langstream tenant here otherwise we won't get the logs in case of test
        // failed
        cleanupEnv();
    }

    private static void cleanupAllEndToEndTestsNamespaces() {
        if (client != null) {
            client.namespaces().withLabel("app", "ls-test").delete();
            client.namespaces().list().getItems().stream()
                    .map(ns -> ns.getMetadata().getName())
                    .filter(ns -> ns.startsWith(TENANT_NAMESPACE_PREFIX))
                    .forEach(ns -> deleteTenantNamespace(ns));
        }
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
    protected static void installLangStreamCluster(boolean authentication) {
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
                LANGSTREAM_REPO != null
                        ? LANGSTREAM_REPO
                        : (LANGSTREAM_TAG.equals("latest-dev")
                                ? "langstream"
                                : "ghcr.io/langstream");

        final String imagePullPolicy =
                switch (LANGSTREAM_TAG) {
                    case "latest-dev" -> "Never";
                    default -> "IfNotPresent";
                };
        final Map<String, String> controlPlaneConfig = new HashMap<>();
        if (authentication) {
            KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
            kpg.initialize(2048);
            controlPlaneAuthKeyPair = kpg.generateKeyPair();
            final String publicKeyEncoded =
                    Base64.getEncoder()
                            .encodeToString(controlPlaneAuthKeyPair.getPublic().getEncoded());
            controlPlaneConfig.put("application.security.enabled", "true");
            controlPlaneConfig.put("application.security.token.public-key", publicKeyEncoded);
            controlPlaneConfig.put("application.security.token.auth-claim", "sub");
            controlPlaneConfig.put("application.security.token.admin-roles", "super-admin");
        } else {
            controlPlaneAuthKeyPair = null;
        }

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
                            config: %s

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
                                cpuPerUnit: %s
                                memPerUnit: %s
                                storageClassesMapping:
                                    default: standard
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
                             logging.level.ai.langstream.apigateway.websocket: debug

                        runtime:
                            image: %s/langstream-runtime
                            imagePullPolicy: %s
                        tenants:
                            defaultTenant:
                                create: false
                            namespacePrefix: %s
                        codeStorage:
                          type: %s
                          configuration: %s
                        """
                        .formatted(
                                LANGSTREAM_TAG,
                                baseImageRepository,
                                imagePullPolicy,
                                JSON_MAPPER.writeValueAsString(controlPlaneConfig),
                                baseImageRepository,
                                imagePullPolicy,
                                LANGSTREAM_APPS_RESOURCES_CPU,
                                LANGSTREAM_APPS_RESOURCES_MEM,
                                baseImageRepository,
                                imagePullPolicy,
                                baseImageRepository,
                                imagePullPolicy,
                                baseImageRepository,
                                imagePullPolicy,
                                TENANT_NAMESPACE_PREFIX,
                                codeStorageConfig.type(),
                                JSON_MAPPER.writeValueAsString(codeStorageConfig.configuration()));

        log.info("Applying values: {}", values);
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
        final String deploymentName = "langstream-control-plane";
        awaitDeploymentReady(deploymentName);
        log.info("control plane ready");
    }

    private static void awaitDeploymentReady(String deploymentName) {
        Awaitility.await()
                .pollInterval(Duration.ofSeconds(10))
                .pollDelay(Duration.ZERO)
                .atMost(2, TimeUnit.MINUTES)
                .until(
                        () -> {
                            final Deployment deployment =
                                    client.apps()
                                            .deployments()
                                            .inNamespace(namespace)
                                            .withName(deploymentName)
                                            .get();
                            if (deployment == null) {
                                return false;
                            }
                            final Pod pod = getFirstPodFromDeployment(deploymentName);
                            if (pod == null) {
                                return false;
                            }
                            return checkPodReadiness(pod);
                        });
    }

    private static boolean checkPodReadiness(Pod pod) {
        final boolean ready = Readiness.getInstance().isReady(pod);
        if (!ready) {
            String podLogs;
            try {
                podLogs =
                        getPodLogs(
                                pod.getMetadata().getName(), pod.getMetadata().getNamespace(), 30);
            } catch (Throwable e) {
                podLogs = "failed to get pod logs: " + e.getMessage();
            }
            log.info("pod {} not ready, logs:\n{}", pod.getMetadata().getName(), podLogs);
            return false;
        }
        return true;
    }

    @SneakyThrows
    private static void awaitApiGatewayReady() {
        log.info("waiting for api gateway to be ready");
        awaitDeploymentReady("langstream-api-gateway");
        log.info("api gateway ready");
    }

    protected static String getPodLogs(String podName, String namespace, int tailingLines) {
        final StringBuilder sb = new StringBuilder();
        withPodLogs(
                podName,
                namespace,
                tailingLines,
                (container, logs) -> {
                    if (!logs.isBlank()) {
                        sb.append("container: ").append(container).append("\n");
                        sb.append(logs).append("\n");
                    }
                });
        return sb.toString();
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
                        final Loggable loggable =
                                tailingLines > 0
                                        ? client.pods()
                                                .inNamespace(namespace)
                                                .withName(podName)
                                                .inContainer(container.getName())
                                                .tailingLines(tailingLines)
                                        : client.pods()
                                                .inNamespace(namespace)
                                                .withName(podName)
                                                .inContainer(container.getName());
                        final String containerLog = loggable.getLog();
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
                                    "%s.%s.%s.%s.log"
                                            .formatted(filePrefix, namespace, podName, container));
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
        dumpResources(filePrefix, Deployment.class, namespaces);
        dumpResources(filePrefix, Secret.class, namespaces);
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
                        "%s.%s.%s.txt"
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
        if (controlPlaneAuthKeyPair != null) {
            final String adminToken = generateControlPlaneAdminToken();
            final String token = generateControlPlaneAdminToken();
            executeCommandOnClient(
                    """
                            bin/langstream configure token %s &&
                            bin/langstream tenants put %s &&
                            bin/langstream configure tenant %s &&
                            bin/langstream configure token %s"""
                            .formatted(adminToken, tenant, tenant, token)
                            .replace(System.lineSeparator(), " ")
                            .split(" "));
        } else {
            executeCommandOnClient(
                    """
                            bin/langstream tenants put %s &&
                            bin/langstream configure tenant %s"""
                            .formatted(tenant, tenant)
                            .replace(System.lineSeparator(), " ")
                            .split(" "));
        }
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
        return replicasReady.equals(
                expectedRunningTotalExecutors + "/" + expectedRunningTotalExecutors);
    }

    protected static void awaitApplicationInStatus(String applicationId, String status) {
        Awaitility.await()
                .atMost(3, TimeUnit.MINUTES)
                .pollInterval(5, TimeUnit.SECONDS)
                .until(() -> isApplicationInStatus(applicationId, status));
    }

    protected static boolean isApplicationInStatus(String applicationId, String expectedStatus) {
        final String response =
                executeCommandOnClient(
                        "bin/langstream apps get %s".formatted(applicationId).split(" "));
        final List<String> lines = response.lines().collect(Collectors.toList());
        final String appLine = lines.get(1);
        final List<String> lineAsList =
                Arrays.stream(appLine.split(" "))
                        .filter(s -> !s.isBlank())
                        .collect(Collectors.toList());
        final String status = lineAsList.get(3);
        if (status != null && status.equals(expectedStatus)) {
            return true;
        }
        log.info(
                "application {} is not in expected status {} but is in {}, dumping:",
                applicationId,
                expectedStatus,
                status);
        executeCommandOnClient(
                "bin/langstream apps get %s -o yaml".formatted(applicationId).split(" "));
        return false;
    }

    @SneakyThrows
    protected static void deployLocalApplicationAndAwaitReady(
            String tenant,
            String applicationId,
            String appDirName,
            Map<String, String> env,
            int expectedNumExecutors) {
        deployLocalApplicationAndAwaitReady(
                tenant, false, applicationId, appDirName, env, expectedNumExecutors, false);
    }

    protected static void updateLocalApplicationAndAwaitReady(
            String tenant,
            String applicationId,
            String appDirName,
            Map<String, String> env,
            int expectedNumExecutors) {
        updateLocalApplicationAndAwaitReady(
                tenant, applicationId, appDirName, env, expectedNumExecutors, false);
    }

    @SneakyThrows
    protected static void updateLocalApplicationAndAwaitReady(
            String tenant,
            String applicationId,
            String appDirName,
            Map<String, String> env,
            int expectedNumExecutors,
            boolean forceRestart) {
        deployLocalApplicationAndAwaitReady(
                tenant, true, applicationId, appDirName, env, expectedNumExecutors, forceRestart);
    }

    @SneakyThrows
    private static void deployLocalApplicationAndAwaitReady(
            String tenant,
            boolean isUpdate,
            String applicationId,
            String appDirName,
            Map<String, String> env,
            int expectedNumExecutors,
            boolean forceRestart) {
        final String tenantNamespace = TENANT_NAMESPACE_PREFIX + tenant;
        final String podUids =
                deployLocalApplication(
                        tenant,
                        isUpdate,
                        applicationId,
                        appDirName,
                        instanceFile,
                        env,
                        forceRestart);

        awaitApplicationReady(applicationId, expectedNumExecutors);
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .pollDelay(Duration.ZERO)
                .pollInterval(5, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            final List<Pod> pods =
                                    client.pods()
                                            .inNamespace(tenantNamespace)
                                            .withLabels(
                                                    Map.of(
                                                            "langstream-application",
                                                            applicationId,
                                                            "app",
                                                            "langstream-runtime"))
                                            .list()
                                            .getItems();
                            log.info(
                                    "waiting new executors to be ready, found {}, expected {}",
                                    pods.size(),
                                    expectedNumExecutors);
                            if (pods.size() != expectedNumExecutors) {
                                fail("too many pods: " + pods.size());
                            }
                            final String currentUids =
                                    pods.stream()
                                            .map(p -> p.getMetadata().getUid())
                                            .sorted()
                                            .collect(Collectors.joining(","));
                            Assertions.assertNotEquals(podUids, currentUids);
                            for (Pod pod : pods) {
                                log.info("checking pod readiness {}", pod.getMetadata().getName());
                                assertTrue(checkPodReadiness(pod));
                            }
                        });
    }

    @SneakyThrows
    protected static String deployLocalApplication(
            String tenant,
            boolean isUpdate,
            String applicationId,
            String appDirName,
            File instanceFile,
            Map<String, String> env) {
        return deployLocalApplication(
                tenant, isUpdate, applicationId, appDirName, instanceFile, env, false);
    }

    @SneakyThrows
    protected static String deployLocalApplication(
            String tenant,
            boolean isUpdate,
            String applicationId,
            String appDirName,
            File instanceFile,
            Map<String, String> env,
            boolean forceRestart) {
        final String tenantNamespace = TENANT_NAMESPACE_PREFIX + tenant;
        String testAppsBaseDir = "src/test/resources/apps";
        String testSecretBaseDir = "src/test/resources/secrets";

        final File appDir = Paths.get(testAppsBaseDir, appDirName).toFile();
        final File secretFile = Paths.get(testSecretBaseDir, "secret1.yaml").toFile();
        validateApp(appDir, secretFile);
        copyFileToClientContainer(appDir, "/tmp/app");
        copyFileToClientContainer(instanceFile, "/tmp/instance.yaml");

        copyFileToClientContainer(secretFile, "/tmp/secrets.yaml");

        String beforeCmd = "";
        if (env != null && !env.isEmpty()) {
            beforeCmd =
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
        }

        final String podUids;
        if (isUpdate) {
            podUids =
                    client
                            .pods()
                            .inNamespace(tenantNamespace)
                            .withLabels(
                                    Map.of(
                                            "langstream-application",
                                            applicationId,
                                            "app",
                                            "langstream-runtime"))
                            .list()
                            .getItems()
                            .stream()
                            .map(p -> p.getMetadata().getUid())
                            .sorted()
                            .collect(Collectors.joining(","));
        } else {
            podUids = "";
        }
        final String forceRestartFlag = isUpdate && forceRestart ? "--force-restart" : "";
        final String command =
                "bin/langstream apps %s %s %s -app /tmp/app -i /tmp/instance.yaml -s /tmp/secrets.yaml"
                        .formatted(isUpdate ? "update" : "deploy", applicationId, forceRestartFlag);
        String logs = executeCommandOnClient((beforeCmd + command).split(" "));
        log.info("Logs after deploy: {}", logs);
        return podUids;
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

    protected static String generateControlPlaneAdminToken() {
        return generateControlPlaneToken("super-admin");
    }

    protected static String generateControlPlaneToken(String subject) {
        if (controlPlaneAuthKeyPair == null) {
            throw new IllegalStateException(
                    "controlPlaneAuthKeyPair is null, no auth configured in this test ?");
        }
        return Jwts.builder()
                .claim("sub", subject)
                .signWith(controlPlaneAuthKeyPair.getPrivate())
                .compact();
    }

    protected void deleteAppAndAwaitCleanup(String tenant, String applicationId) {
        executeCommandOnClient("bin/langstream apps delete %s".formatted(applicationId).split(" "));

        awaitApplicationCleanup(tenant, applicationId);
    }

    protected static void awaitApplicationCleanup(String tenant, String applicationId) {
        final String tenantNamespace = TENANT_NAMESPACE_PREFIX + tenant;
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(
                                    0,
                                    client.apps()
                                            .statefulSets()
                                            .inNamespace(tenantNamespace)
                                            .withLabel("langstream-application", applicationId)
                                            .list()
                                            .getItems()
                                            .size());

                            Assertions.assertEquals(
                                    0,
                                    client.resources(AgentCustomResource.class)
                                            .inNamespace(tenantNamespace)
                                            .list()
                                            .getItems()
                                            .size());

                            Assertions.assertEquals(
                                    0,
                                    client.resources(ApplicationCustomResource.class)
                                            .inNamespace(tenantNamespace)
                                            .list()
                                            .getItems()
                                            .size());

                            Assertions.assertEquals(
                                    1,
                                    client.resources(Secret.class)
                                            .inNamespace(tenantNamespace)
                                            .list()
                                            .getItems()
                                            .size());
                        });
    }
}
