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
import io.fabric8.kubernetes.client.Config;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import lombok.SneakyThrows;

public class RunningHostCluster implements KubeCluster {
    @Override
    public void start() {}

    @Override
    public void ensureImage(String image) {}

    @Override
    public void stop() {}

    @Override
    @SneakyThrows
    public String getKubeConfig() {
        final String kubeConfig = Config.getKubeconfigFilename();
        return Files.readString(Paths.get(kubeConfig), StandardCharsets.UTF_8);
    }
}
