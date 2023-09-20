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
package ai.langstream.tests.util.k8s;

import ai.langstream.tests.util.KubeCluster;
import com.dajudge.kindcontainer.K3sContainer;
import com.dajudge.kindcontainer.K3sContainerVersion;
import com.dajudge.kindcontainer.KubernetesImageSpec;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.BindMode;

@Slf4j
public class LocalK3sContainer implements KubeCluster {

    K3sContainer container;
    final Path basePath = Paths.get("/tmp", "ls-tests-image");

    @Override
    public void start() {
        container =
                new K3sContainer(
                        new KubernetesImageSpec<>(K3sContainerVersion.VERSION_1_25_0)
                                .withImage("rancher/k3s:v1.25.3-k3s1"));
        container.withFileSystemBind(
                basePath.toFile().getAbsolutePath(), "/images", BindMode.READ_WRITE);
        // container.withNetwork(network);
        container.start();
    }

    @Override
    public void ensureImage(String image) {
        loadImage(basePath, image);
    }

    @SneakyThrows
    private void loadImage(Path basePath, String image) {
        final String id =
                DockerClientFactory.lazyClient()
                        .inspectImageCmd(image)
                        .exec()
                        .getId()
                        .replace("sha256:", "");

        final Path hostPath = basePath.resolve(id);
        if (!hostPath.toFile().exists()) {
            log.info("Saving image {} locally", image);
            final InputStream in = DockerClientFactory.lazyClient().saveImageCmd(image).exec();

            try (final OutputStream outputStream = Files.newOutputStream(hostPath)) {
                in.transferTo(outputStream);
            } catch (Exception ex) {
                hostPath.toFile().delete();
                throw ex;
            }
        }

        log.info("Loading image {} in the k3s container", image);
        if (container.execInContainer("ctr", "images", "import", "/images/" + id).getExitCode()
                != 0) {
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
