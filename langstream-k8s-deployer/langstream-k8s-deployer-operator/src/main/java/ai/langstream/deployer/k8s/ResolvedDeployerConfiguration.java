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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import jakarta.inject.Singleton;
import java.util.Map;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.jbosslog.JBossLog;

@Singleton
@JBossLog
public class ResolvedDeployerConfiguration {

    private static final ObjectMapper yamlMapper =
            new ObjectMapper(
                            YAMLFactory.builder()
                                    .enable(YAMLGenerator.Feature.MINIMIZE_QUOTES)
                                    .disable(YAMLGenerator.Feature.SPLIT_LINES)
                                    .build())
                    .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                    .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    @SneakyThrows
    public ResolvedDeployerConfiguration(DeployerConfiguration configuration) {
        this.clusterRuntime = yamlMapper.readValue(configuration.clusterRuntime(), Map.class);
        this.agentResources =
                yamlMapper.readValue(
                        configuration.agentResources(), AgentResourceUnitConfiguration.class);
        final PodTemplate commonPodTemplate =
                yamlMapper.readValue(configuration.podTemplate(), PodTemplate.class);

        this.agentPodTemplate =
                PodTemplate.merge(
                        yamlMapper.readValue(configuration.agentPodTemplate(), PodTemplate.class),
                        commonPodTemplate);

        this.appDeployerPodTemplate =
                PodTemplate.merge(
                        yamlMapper.readValue(
                                configuration.appDeployerPodTemplate(), PodTemplate.class),
                        commonPodTemplate);

        this.runtimeImage = configuration.runtimeImage().orElse(null);
        this.runtimeImagePullPolicy = configuration.runtimeImagePullPolicy().orElse(null);
        this.globalStorageConfiguration =
                yamlMapper.readValue(
                        configuration.globalStorage(), GlobalStorageConfiguration.class);
        log.infof("Deployer configuration: %s", SerializationUtil.writeAsJson(this));
    }

    @Getter private Map<String, Object> clusterRuntime;

    @Getter private AgentResourceUnitConfiguration agentResources;

    @Getter private PodTemplate appDeployerPodTemplate;

    @Getter private PodTemplate agentPodTemplate;

    @Getter private String runtimeImage;

    @Getter private String runtimeImagePullPolicy;

    @Getter private GlobalStorageConfiguration globalStorageConfiguration;
}
