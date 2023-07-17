package com.datastax.oss.sga.impl.k8s.tests;

import com.dajudge.kindcontainer.K3sContainer;
import com.dajudge.kindcontainer.K3sContainerVersion;
import com.dajudge.kindcontainer.KubernetesImageSpec;
import com.datastax.oss.sga.impl.k8s.KubernetesClientFactory;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public class KubeK3sServer implements AutoCloseable, BeforeAllCallback, AfterAllCallback {

    private K3sContainer k3s;
    @Getter
    private KubernetesClient client;

    private final boolean installCRDs;

    public KubeK3sServer() {
        this(false);
    }

    public KubeK3sServer(boolean installCRDs) {
        this.installCRDs = installCRDs;
    }

    @Override
    public void close() throws Exception {
        if (k3s != null) {
            k3s.stop();
        }
        KubernetesClientFactory.clear();
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) throws Exception {
        close();

    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {

        k3s = new K3sContainer(new KubernetesImageSpec<>(K3sContainerVersion.VERSION_1_25_0)
                .withImage("rancher/k3s:v1.25.3-k3s1"));
        k3s.withLogConsumer(
                (Consumer<OutputFrame>) outputFrame -> log.debug("k3s> {}", outputFrame.getUtf8String().trim()));
        k3s.start();
        client = new KubernetesClientBuilder()
                .withConfig(Config.fromKubeconfig(k3s.getKubeconfig()))
                .build();
        KubernetesClientFactory.set(null, client);

        if (installCRDs) {

            Path basePath = null;

            List<String> locations = List.of("helm", Path.of("..", "helm").toString(), Path.of("..", "..", "helm").toString());

            for (String location : locations) {
                basePath = Path.of(location);
                if (basePath.toFile().exists() && basePath.toFile().isDirectory()) {
                    break;
                }
            }
            if (basePath == null || !basePath.toFile().exists()) {
                throw new IllegalStateException("Could not find helm directory");
            }

            try (final InputStream fin = Files.newInputStream(
                    Path.of(basePath.toFile().getAbsolutePath(), "sga", "crds",
                            "applications.sga.oss.datastax.com-v1.yml"))) {
                client.load(fin).create();
            }
            try (final InputStream fin = Files.newInputStream(
                    Path.of(basePath.toFile().getAbsolutePath(), "sga", "crds",
                            "agents.sga.oss.datastax.com-v1.yml"))) {
                client.load(fin).create();
            }
        }

    }

}
