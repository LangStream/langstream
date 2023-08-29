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
package ai.langstream.deployer.k8s.api.crds.agents;

import ai.langstream.deployer.k8s.api.crds.NamespacedSpec;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class AgentSpec extends NamespacedSpec {

    public record Resources(int parallelism, int size) {}

    private String agentId;
    private String applicationId;
    @Deprecated private String image;
    @Deprecated private String imagePullPolicy;
    private String agentConfigSecretRef;
    private String agentConfigSecretRefChecksum;
    private Resources resources;
    private String codeArchiveId;

    @Builder
    public AgentSpec(
            String tenant,
            String agentId,
            String applicationId,
            String image,
            String imagePullPolicy,
            String agentConfigSecretRef,
            String agentConfigSecretRefChecksum,
            Resources resources,
            String codeArchiveId) {
        super(tenant);
        this.agentId = agentId;
        this.applicationId = applicationId;
        this.image = image;
        this.imagePullPolicy = imagePullPolicy;
        this.agentConfigSecretRef = agentConfigSecretRef;
        this.agentConfigSecretRefChecksum = agentConfigSecretRefChecksum;
        this.resources = resources;
        this.codeArchiveId = codeArchiveId;
    }
}
