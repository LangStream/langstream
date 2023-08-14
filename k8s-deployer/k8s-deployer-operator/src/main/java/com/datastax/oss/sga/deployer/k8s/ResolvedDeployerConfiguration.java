/**
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
package com.datastax.oss.sga.deployer.k8s;

import com.datastax.oss.sga.deployer.k8s.agents.AgentResourceUnitConfiguration;
import com.datastax.oss.sga.deployer.k8s.util.SerializationUtil;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.client.utils.Serialization;
import io.smallrye.config.WithDefault;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.Map;
import lombok.Getter;

@ApplicationScoped
public class ResolvedDeployerConfiguration {

    public ResolvedDeployerConfiguration(DeployerConfiguration configuration) {
        this.clusterRuntime = SerializationUtil.readYaml(configuration.clusterRuntime(), Map.class);
        this.codeStorage = SerializationUtil.readYaml(configuration.codeStorage(), Map.class);
        this.agentResources = SerializationUtil.readYaml(configuration.agentResources(), AgentResourceUnitConfiguration.class);
        this.podTemplate = SerializationUtil.readYaml(configuration.podTemplate(), PodTemplate.class);
    }

    @Getter
    private Map<String, Object> clusterRuntime;

    @Getter
    private Map<String, Object> codeStorage;

    @Getter
    private AgentResourceUnitConfiguration agentResources;

    @Getter
    private PodTemplate podTemplate;

}
