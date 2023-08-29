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
package ai.langstream.impl.k8s.tests;

import static org.mockito.ArgumentMatchers.isNull;

import ai.langstream.impl.k8s.KubernetesClientFactory;
import com.dajudge.kindcontainer.K3sContainer;
import com.dajudge.kindcontainer.K3sContainerVersion;
import com.dajudge.kindcontainer.KubernetesImageSpec;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testcontainers.containers.output.OutputFrame;

@Slf4j
public class KubeK3sServer implements AutoCloseable, BeforeAllCallback, AfterAllCallback {

    @Getter private K3sContainer k3sContainer;
    @Getter private KubernetesClient client;
    private MockedStatic mocked;

    private final boolean installCRDs;

    public KubeK3sServer() {
        this(false);
    }

    public KubeK3sServer(boolean installCRDs) {
        this.installCRDs = installCRDs;
    }

    @Override
    public void close() throws Exception {
        if (k3sContainer != null) {
            k3sContainer.stop();
        }
        if (client != null) {
            client.close();
        }
        if (mocked != null) {
            mocked.close();
        }
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) throws Exception {
        close();
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {

        k3sContainer =
                new K3sContainer(
                        new KubernetesImageSpec<>(K3sContainerVersion.VERSION_1_25_0)
                                .withImage("rancher/k3s:v1.25.3-k3s1"));
        k3sContainer.withLogConsumer(
                (Consumer<OutputFrame>)
                        outputFrame -> log.debug("k3s> {}", outputFrame.getUtf8String().trim()));
        k3sContainer.start();
        final Path tempFile = Files.createTempFile("langstream-test-kube", ".yaml");
        Files.writeString(tempFile, k3sContainer.getKubeconfig());
        System.out.println(
                "To inspect the container\nKUBECONFIG="
                        + tempFile.toFile().getAbsolutePath()
                        + " k9s");
        client =
                new KubernetesClientBuilder()
                        .withConfig(Config.fromKubeconfig(k3sContainer.getKubeconfig()))
                        .build();

        mocked = Mockito.mockStatic(KubernetesClientFactory.class);
        mocked.when(() -> KubernetesClientFactory.get(isNull())).thenReturn(client);
        mocked.when(() -> KubernetesClientFactory.create(isNull())).thenReturn(client);

        if (installCRDs) {

            Path basePath = null;

            List<String> locations =
                    List.of(
                            "helm",
                            Path.of("..", "helm").toString(),
                            Path.of("..", "..", "helm").toString());

            for (String location : locations) {
                basePath = Path.of(location);
                if (basePath.toFile().exists() && basePath.toFile().isDirectory()) {
                    break;
                }
            }
            if (!basePath.toFile().exists()) {
                throw new IllegalStateException("Could not find helm directory");
            }

            try (final InputStream fin =
                    Files.newInputStream(
                            Path.of(
                                    basePath.toFile().getAbsolutePath(),
                                    "crds",
                                    "applications.langstream.ai-v1.yml"))) {
                client.load(fin).create();
            }
            try (final InputStream fin =
                    Files.newInputStream(
                            Path.of(
                                    basePath.toFile().getAbsolutePath(),
                                    "crds",
                                    "agents.langstream.ai-v1.yml"))) {
                client.load(fin).create();
            }
        }
    }
}
