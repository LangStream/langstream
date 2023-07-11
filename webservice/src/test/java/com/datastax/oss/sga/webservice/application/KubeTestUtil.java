package com.datastax.oss.sga.webservice.application;

import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.k3s.K3sContainer;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public abstract class KubeTestUtil {

    static K3sContainer k3s;
    static Path kubeconfigFile;

    @BeforeAll
    public static void setup(@TempDir Path tempDir) throws Exception {
        k3s = new K3sContainer(DockerImageName.parse("rancher/k3s:v1.21.3-k3s1"))
                .withLogConsumer(outputFrame -> log.info("k3s> {}", outputFrame.getUtf8String().trim()));
        k3s.start();
        kubeconfigFile = tempDir.resolve("kubeconfig.yaml");
        Files.writeString(kubeconfigFile, k3s.getKubeConfigYaml());
        System.setProperty(Config.KUBERNETES_KUBECONFIG_FILE, kubeconfigFile.toFile().getAbsolutePath());
        try (final KubernetesClient client = new KubernetesClientBuilder()
                .withConfig(Config.fromKubeconfig(k3s.getKubeConfigYaml()))
                .build();) {
            client.load(Files.newInputStream(Path.of("../helm/sga/crds/applications.sga.oss.datastax.com-v1.yml")))
                    .create();
        }
    }


    @AfterAll
    public static void after() {
        System.clearProperty(Config.KUBERNETES_KUBECONFIG_FILE);
        if (k3s != null) {
            k3s.stop();
        }
    }
}
