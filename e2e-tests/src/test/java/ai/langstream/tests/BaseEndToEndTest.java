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
package ai.langstream.tests;

import com.dajudge.kindcontainer.K3sContainer;
import com.dajudge.kindcontainer.K3sContainerVersion;
import com.dajudge.kindcontainer.KubernetesImageSpec;
import com.dajudge.kindcontainer.helm.Helm3Container;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.rbac.ClusterRole;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.BindMode;

@Slf4j
public abstract class BaseEndToEndTest {

    protected static final String TENANT_NAMESPACE_PREFIX = "langstream-tenant-";

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
        final Path basePath = Paths.get("/tmp", "langstream-tests-image");

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
    protected static String namespace;

    protected void receiveKafkaMessages(String topic, int count) {
        applyManifest("""
                apiVersion: batch/v1
                kind: Job
                metadata:
                  name: kafka-client-consumer
                  labels:
                    role: kafka-client-consumer
                spec:
                  template:
                    metadata:
                      labels:
                        role: kafka-client-consumer
                    spec:
                      restartPolicy: OnFailure
                      containers:
                        - name: kclient
                          image: confluentinc/cp-kafka
                          command: ["/bin/sh"]
                          args:
                            - "-c"
                            - >-
                              set -e &&
                              echo 'bootstrap.servers=my-cluster-kafka-bootstrap.kafka:9092\\n' >> consumer-props.conf &&
                              kafka-consumer-perf-test  --bootstrap-server my-cluster-kafka-bootstrap.kafka:9092 --topic %s --timeout 240000 --consumer.config consumer-props.conf --print-metrics --messages %d --show-detailed-stats --reporting-interval 1000
                """.formatted(topic, count));

        client.batch().v1()
                .jobs()
                .inNamespace(namespace)
                .withName("kafka-client-consumer")
                .waitUntilCondition(
                        pod -> pod.getStatus().getSucceeded() != null && pod.getStatus().getSucceeded() == 1, 2,
                        TimeUnit.MINUTES);
    }

    protected void produceKafkaMessages(String topic, int count) {
        applyManifest("""
                apiVersion: apps/v1
                kind: Deployment
                metadata:
                  name: kafka-client-producer
                  labels:
                    role: kafka-client-producer
                spec:
                  replicas: 1
                  selector:
                    matchLabels:
                      role: kafka-client-producer
                  template:
                    metadata:
                      labels:
                        role: kafka-client-producer
                    spec:
                      containers:
                        - name: kclient
                          image: confluentinc/cp-kafka
                          command: ["/bin/sh"]
                          args:
                            - "-c"
                            - >-
                              echo 'bootstrap.servers=my-cluster-kafka-bootstrap.kafka:9092\\n' >> producer.conf &&
                              kafka-producer-perf-test --topic %s --num-records %d --record-size 10240 --throughput 1000 --producer.config producer.conf
                """.formatted(topic, count));
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
        allArgs[0] = "bin/langstream";
        System.arraycopy(args, 0, allArgs, 1, args.length);
        runProcess(allArgs);
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
            throw new RuntimeException();
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
        namespace = "langstream-test-" + UUID.randomUUID().toString().substring(0, 8);

        // kubeServer = new LocalK3sContainer();
        kubeServer = new PythonFunctionIT.RunningHostCluster();
        kubeServer.start();

        client = new KubernetesClientBuilder()
                .withConfig(Config.fromKubeconfig(kubeServer.getKubeConfig()))
                .build();

        // cleanup previous runs
        client.namespaces().withLabel("app", "langstream-test")
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
                        .withLabels(Map.of("app", "langstream-test"))
                        .endMetadata()
                        .build())
                .serverSideApply();


        final Path tempFile = Files.createTempFile("langstream-test-kube", ".yaml");
        Files.write(tempFile,
                kubeServer.getKubeConfig().getBytes(StandardCharsets.UTF_8));
        System.out.println("To inspect the container\nKUBECONFIG=" + tempFile.toFile().getAbsolutePath() + " k9s");

        final CompletableFuture<Void> kafkaFuture = CompletableFuture.runAsync(() -> installKafka());
        final CompletableFuture<Void> minioFuture = CompletableFuture.runAsync(() -> installMinio());
        List<CompletableFuture<Void>> imagesFutures = new ArrayList<>();

        imagesFutures.add(CompletableFuture.runAsync(() ->
                kubeServer.ensureImage("datastax/langstream-control-plane:latest-dev")));
        imagesFutures.add(CompletableFuture.runAsync(() ->
                kubeServer.ensureImage( "datastax/langstream-deployer:latest-dev")));
        imagesFutures.add(CompletableFuture.runAsync(() ->
                kubeServer.ensureImage("datastax/langstream-runtime:latest-dev")));

        final CompletableFuture<Void> serviceFuture =
                CompletableFuture.runAsync(() -> installAndPrepareControlPlaneUrl());
        CompletableFuture.allOf(kafkaFuture, minioFuture, imagesFutures.get(0), imagesFutures.get(1), imagesFutures.get(2),
                serviceFuture).join();
        awaitControlPlaneReady();
    }

    @SneakyThrows
    private static void installAndPrepareControlPlaneUrl() {
        final String hostPath = Paths.get("..", "helm", "langstream").toFile().getAbsolutePath();

        client.resources(ClusterRole.class)
                        .withName("langstream-deployer")
                        .delete();
        client.resources(ClusterRole.class)
                .withName("langstream-control-plane")
                .delete();
        client.resources(ClusterRoleBinding.class)
                .withName("langstream-control-plane-role-binding")
                .delete();
        client.resources(ClusterRoleBinding.class)
                .withName("langstream-deployer-role-binding")
                .delete();


        log.info("installing langstream with helm, using chart from {}", hostPath);
        final String deleteCmd =
                "helm delete %s -n %s".formatted("langstream", namespace);
        log.info("Running {}", deleteCmd);
        runProcess(deleteCmd.split(" "), true);

        final String values = """
                controlPlane:
                  resources:
                    requests:
                      cpu: 500m
                      memory: 512Mi
                  app:
                    config:
                      application.storage.apps.type: kubernetes
                      application.storage.apps.configuration.namespaceprefix: %s
                      application.storage.apps.configuration.deployer-runtime.image: datastax/langstream-runtime:latest-dev
                      application.storage.apps.configuration.deployer-runtime.image-pull-policy: Never
                      application.storage.global.type: kubernetes
                      application.storage.code.type: s3
                      application.storage.code.configuration.endpoint: http://minio.minio-dev.svc.cluster.local:9000
                      application.storage.code.configuration.access-key: minioadmin
                      application.storage.code.configuration.secret-key: minioadmin
                                
                deployer:
                  replicaCount: 1
                  app:
                    config:
                      clusterRuntime:
                          kubernetes:
                            namespace-prefix: %s
                      codeStorage:
                        type: s3
                        endpoint: http://minio.minio-dev.svc.cluster.local:9000
                        access-key: minioadmin
                        secret-key: minioadmin
                """.formatted(TENANT_NAMESPACE_PREFIX, TENANT_NAMESPACE_PREFIX);
        final Path tempFile = Files.createTempFile("langstream-test", ".yaml");
        Files.write(tempFile, values.getBytes(StandardCharsets.UTF_8));


        final String cmd =
                "helm install --debug --timeout 360s %s -n %s %s --values %s".formatted(
                        "langstream", namespace, hostPath, tempFile.toFile().getAbsolutePath());
        log.info("Running {}", cmd);
        runProcess(cmd.split(" "));
        log.info("Helm install completed");

    }

    private static void awaitControlPlaneReady() {
        log.info("waiting for control plane to be ready");

        client.apps().deployments().inNamespace(namespace).withName("langstream-control-plane")
                .waitUntilCondition(
                        d -> d.getStatus().getReadyReplicas() != null && d.getStatus().getReadyReplicas() == 1, 120,
                        TimeUnit.SECONDS);
        log.info("control plane ready, port forwarding");

        final String podName = client.pods().inNamespace(namespace).withLabel("app.kubernetes.io/name", "langstream-control-plane")
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


}
