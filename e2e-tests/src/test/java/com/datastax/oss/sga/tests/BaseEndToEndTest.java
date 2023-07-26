package com.datastax.oss.sga.tests;

import com.dajudge.kindcontainer.K3sContainer;
import com.dajudge.kindcontainer.K3sContainerVersion;
import com.dajudge.kindcontainer.KubernetesImageSpec;
import com.dajudge.kindcontainer.helm.Helm3Container;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public abstract class BaseEndToEndTest {

    interface KubeServer {
        void start();

        void ensureImage(String image);

        void stop();

        String getKubeConfig();

        Helm3Container setupHelmContainer();
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

        @Override
        public Helm3Container setupHelmContainer() {
            final Helm3Container helm3Container = new Helm3Container(DockerImageName.parse("alpine/helm:3.7.2"),
                    () -> kubeServer.getKubeConfig());
            return helm3Container;
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

        @Override
        public Helm3Container setupHelmContainer() {
            final Helm3Container helm3Container = new Helm3Container(DockerImageName.parse("alpine/helm:3.7.2"),
                    () -> kubeServer.getKubeConfig());
            PythonFunctionIT.helm3Container.withNetworkMode("container:" + container.getContainerId());
            return helm3Container;
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
                              kafka-consumer-perf-test --topic %s --timeout 240000 --consumer.config consumer-props.conf --print-metrics --from-earliest --messages %d --show-detailed-stats --reporting-interval 1000
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

    protected void executeCliCommand(String... args)
            throws InterruptedException, IOException {

        String[] allArgs = new String[args.length + 1];
        allArgs[0] = "bin/sga-cli";
        System.arraycopy(args, 0, allArgs, 1, args.length);
        runProcess(allArgs);
    }

    private static void runProcess(String[] allArgs) throws InterruptedException, IOException {
        ProcessBuilder processBuilder = new ProcessBuilder(allArgs)
                .directory(Paths.get("..").toFile())
                .inheritIO()
                .redirectOutput(ProcessBuilder.Redirect.INHERIT)
                .redirectError(ProcessBuilder.Redirect.INHERIT);
        final int exitCode = processBuilder.start().waitFor();
        if (exitCode != 0) {
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
        namespace = "sga-" + UUID.randomUUID().toString().substring(0, 8);

        // kubeServer = new LocalK3sContainer();
        kubeServer = new PythonFunctionIT.RunningHostCluster();
        kubeServer.start();

        client = new KubernetesClientBuilder()
                .withConfig(Config.fromKubeconfig(kubeServer.getKubeConfig()))
                .build();

        client.resource(new NamespaceBuilder()
                        .withNewMetadata()
                        .withName(namespace)
                        .endMetadata()
                        .build())
                .serverSideApply();


        final Path tempFile = Files.createTempFile("sga-test-kube", ".yaml");
        Files.write(tempFile,
                kubeServer.getKubeConfig().getBytes(StandardCharsets.UTF_8));
        System.out.println("To inspect the container\nKUBECONFIG=" + tempFile.toFile().getAbsolutePath() + " k9s");

        final CompletableFuture<Void> kafkaFuture = CompletableFuture.runAsync(() -> installKafka());
        List<CompletableFuture<Void>> imagesFutures = new ArrayList<>();

        imagesFutures.add(CompletableFuture.runAsync(() ->
                kubeServer.ensureImage("datastax/sga-control-plane:latest-dev")));
        imagesFutures.add(CompletableFuture.runAsync(() ->
                kubeServer.ensureImage( "datastax/sga-deployer:latest-dev")));
        imagesFutures.add(CompletableFuture.runAsync(() ->
                kubeServer.ensureImage("datastax/sga-runtime:latest-dev")));

        final CompletableFuture<Void> sgaFuture =
                CompletableFuture.runAsync(() -> installSgaAndPrepareControlPlaneUrl());
        CompletableFuture.allOf(kafkaFuture, imagesFutures.get(0), imagesFutures.get(1), imagesFutures.get(2),
                sgaFuture).join();
        awaitControlPlaneReady();
    }

    @SneakyThrows
    private static void installSgaAndPrepareControlPlaneUrl() {
        final String hostPath = Paths.get("..", "helm", "sga").toFile().getAbsolutePath();
        log.info("installing sga with helm, using chart from {}", hostPath);
        final String deleteCmd =
                "helm delete %s -n %s".formatted("sga", namespace);
        log.info("Running {}", deleteCmd);
        runProcess(deleteCmd.split(" "));
        final String cmd =
                "helm install --debug --timeout 360s %s -n %s %s".formatted(
                        "sga", namespace, hostPath);
        log.info("Running {}", cmd);
        runProcess(cmd.split(" "));
        log.info("Helm install completed");

    }

    @SneakyThrows
    private static void installSgaAndPrepareControlPlaneUrl0() {
        helm3Container = kubeServer.setupHelmContainer();
        final String hostPath = Paths.get("..", "helm", "sga").toFile().getAbsolutePath();
        log.info("installing sga with helm, using chart from {}", hostPath);
        helm3Container.withFileSystemBind(hostPath, "/charts", BindMode.READ_ONLY);
        helm3Container.start();
        helm3Container.copyFileToContainer(Transferable.of("""
                """), "/test-values.yaml");
        final String cmd =
                "helm install --debug --timeout 360s %s -n %s %s --values /test-values.yaml".formatted(
                        "sga", namespace, "/charts");
        log.info("Running {}", cmd);
        final Container.ExecResult exec = helm3Container.execInContainer(cmd.split(" "));
        if (exec.getExitCode() != 0) {
            throw new RuntimeException("Helm installation failed: " + exec.getStderr());
        }
        log.info("Helm install completed");

    }

    private static void awaitControlPlaneReady() {
        log.info("waiting for control plane to be ready");

        client.apps().deployments().inNamespace(namespace).withName("sga-control-plane")
                .waitUntilCondition(
                        d -> d.getStatus().getReadyReplicas() != null && d.getStatus().getReadyReplicas() == 1, 60,
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

    @SneakyThrows
    private static void installKafka() {
        log.info("installing kafka");
        runProcess("kubectl create namespace kafka".split(" "));
        runProcess("kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka".split(" "));
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


}
