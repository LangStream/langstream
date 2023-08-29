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

import ai.langstream.deployer.k8s.agents.AgentResourceUnitConfiguration;
import ai.langstream.deployer.k8s.util.SerializationUtil;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Map;
import lombok.Getter;

@ApplicationScoped
public class ResolvedDeployerConfiguration {

    public ResolvedDeployerConfiguration(DeployerConfiguration configuration) {
        this.clusterRuntime = SerializationUtil.readYaml(configuration.clusterRuntime(), Map.class);
        this.agentResources =
                SerializationUtil.readYaml(
                        configuration.agentResources(), AgentResourceUnitConfiguration.class);
        final PodTemplate commonPodTemplate =
                SerializationUtil.readYaml(configuration.podTemplate(), PodTemplate.class);

        this.agentPodTemplate =
                PodTemplate.merge(
                        SerializationUtil.readYaml(
                                configuration.agentPodTemplate(), PodTemplate.class),
                        commonPodTemplate);

        this.appDeployerPodTemplate =
                PodTemplate.merge(
                        SerializationUtil.readYaml(
                                configuration.appDeployerPodTemplate(), PodTemplate.class),
                        commonPodTemplate);

        this.runtimeImage = configuration.runtimeImage().orElse(null);
        this.runtimeImagePullPolicy = configuration.runtimeImagePullPolicy().orElse(null);
    }

    @Getter private Map<String, Object> clusterRuntime;

    @Getter private AgentResourceUnitConfiguration agentResources;

    @Getter private PodTemplate appDeployerPodTemplate;

    @Getter private PodTemplate agentPodTemplate;

    @Getter private String runtimeImage;

    @Getter private String runtimeImagePullPolicy;
}
