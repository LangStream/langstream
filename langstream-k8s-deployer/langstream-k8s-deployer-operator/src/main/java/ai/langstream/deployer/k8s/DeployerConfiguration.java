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
package ai.langstream.deployer.k8s;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.util.Optional;

@ConfigMapping(prefix = "deployer")
public interface DeployerConfiguration {

    @WithDefault("{}")
    String globalStorage();

    // workaround: quarkus doesn't support dynamic maps
    @WithDefault("{}")
    String clusterRuntime();

    @WithDefault("{}")
    @Deprecated(forRemoval = true)
    String codeStorage();

    @WithDefault("{}")
    String agentResources();

    @WithDefault("{}")
    String podTemplate();

    @WithDefault("{}")
    String appDeployerPodTemplate();

    @WithDefault("{}")
    String agentPodTemplate();

    Optional<String> runtimeImage();

    Optional<String> runtimeImagePullPolicy();
}
