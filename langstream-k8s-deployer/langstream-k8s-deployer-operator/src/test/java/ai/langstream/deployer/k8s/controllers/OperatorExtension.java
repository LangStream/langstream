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
package ai.langstream.deployer.k8s.controllers;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.langstream.deployer.k8s.LeaderElectionConfig;
import ai.langstream.deployer.k8s.util.SerializationUtil;
import com.dajudge.kindcontainer.K3sContainer;
import com.dajudge.kindcontainer.exception.ExecutionException;
import com.dajudge.kindcontainer.kubectl.KubectlContainer;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PortForwardingContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public class OperatorExtension implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback {

    GenericContainer<?> container;
    K3sContainer k3s;
    KubernetesClient client;
    private final Map<String, String> env;

    public OperatorExtension(Map<String, String> env) {
        this.env = env;
    }

    public OperatorExtension() {
        this(Map.of());
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) {
        if (container != null) {
            container.stop();
            container = null;
        }
        if (client != null) {
            client.close();
            client = null;
        }
        if (k3s != null) {
            k3s.stop();
            k3s = null;
        }
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        MethodUtils.invokeMethod(PortForwardingContainer.INSTANCE, true, "reset");
        k3s = new K3sContainer();
        k3s.start();
        final Path tempFile = Files.createTempFile("langstream-test-kube", ".yaml");
        Files.writeString(tempFile, k3s.getKubeconfig());
        System.out.println(
                "To inspect the container\nKUBECONFIG="
                        + tempFile.toFile().getAbsolutePath()
                        + " k9s");
        applyCRDs();
        Testcontainers.exposeHostPorts(k3s.getFirstMappedPort());
        client =
                new KubernetesClientBuilder()
                        .withConfig(Config.fromKubeconfig(k3s.getKubeconfig()))
                        .build();
    }

    @Override
    public void beforeEach(ExtensionContext extensionContext) throws Exception {
        restartDeployerOperator();
    }

    private void startDeployerOperator() throws IOException {
        final Path kubeconfigFile = writeKubeConfigForOperatorContainer();
        container =
                new GenericContainer<>(
                        DockerImageName.parse("langstream/langstream-deployer:latest-dev"));
        container.withFileSystemBind(
                kubeconfigFile.toFile().getAbsolutePath(), "/tmp/kubeconfig.yaml");
        container.withEnv("KUBECONFIG", "/tmp/kubeconfig.yaml");
        container.withEnv("QUARKUS_KUBERNETES_CLIENT_TRUST_CERTS", "true");
        container.withEnv("KUBERNETES_NAMESPACE", "default");
        container.withEnv(
                "QUARKUS_LOG_CATEGORY__IO_JAVAOPERATORSDK_OPERATOR_PROCESSING_EVENT_SOURCE_CONTROLLER",
                "debug");
        container.withEnv("QUARKUS_LOG_CATEGORY__AI_LANGSTREAM_DEPLOYER_K8S_APPS", "debug");
        env.forEach(container::withEnv);
        container.withExposedPorts(8080);
        container.withAccessToHost(true);
        container.setWaitStrategy(new HttpWaitStrategy().forPort(8080).forPath("/q/health/ready"));
        container.withLogConsumer(
                outputFrame -> System.out.print("operator>" + outputFrame.getUtf8String()));
        container.start();
        final String containerIdentity = container.getContainerId().substring(0, 12);
        Awaitility.await()
                .atMost(Duration.ofMinutes(1))
                .pollInterval(Duration.ofSeconds(5))
                .untilAsserted(
                        () -> {
                            final String identity =
                                    client.leases()
                                            .inNamespace("default")
                                            .withName(LeaderElectionConfig.LEASE_NAME)
                                            .get()
                                            .getSpec()
                                            .getHolderIdentity();
                            log.info(
                                    "found identity: {} expected: {}", identity, containerIdentity);
                            assertEquals(containerIdentity, identity);
                        });
    }

    @SneakyThrows
    public void restartDeployerOperator() {
        if (container != null) {
            container.stop();
            container = null;
        }
        startDeployerOperator();
    }

    public KubernetesClient getClient() {
        return client;
    }

    private void applyCRDs() throws IOException, ExecutionException, InterruptedException {
        final KubectlContainer kubectl = k3s.kubectl();
        Files.list(Path.of("..", "..", "helm", "crds"))
                .forEach(
                        path -> {
                            try {
                                kubectl.copyFileToContainer(
                                        Transferable.of(Files.readAllBytes(path)),
                                        "/crds/" + path.getFileName());
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
        kubectl.apply.from("/crds").run();
    }

    private Path writeKubeConfigForOperatorContainer() throws IOException {
        final Path kubeconfigFile =
                Files.createTempDirectory("test-k3s").resolve("kubeconfig.yaml");

        final Map asMap = SerializationUtil.readYaml(k3s.getInternalKubeconfig(), Map.class);
        ((List<Map<String, Object>>) asMap.get("clusters"))
                .get(0)
                .put(
                        "cluster",
                        Map.of(
                                "server",
                                String.format(
                                        "https://%s:%d",
                                        "host.testcontainers.internal", k3s.getFirstMappedPort()),
                                "insecure-skip-tls-verify",
                                true));
        final String newConfig = SerializationUtil.writeAsYaml(asMap);
        Files.writeString(kubeconfigFile, newConfig);
        return kubeconfigFile;
    }
}
