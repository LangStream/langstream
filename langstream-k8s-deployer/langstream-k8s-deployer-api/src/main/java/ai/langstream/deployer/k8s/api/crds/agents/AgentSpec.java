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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.ToString;

@Setter
@Getter
@ToString
@NoArgsConstructor
public class AgentSpec extends NamespacedSpec {

    public record Resources(int parallelism, int size) {}

    public record Disk(String agentId, long size, String type) {}

    public record Options(
            List<Disk> disks,
            boolean autoUpgradeRuntimeImage,
            boolean autoUpgradeRuntimeImagePullPolicy,
            boolean autoUpgradeAgentResources,
            boolean autoUpgradeAgentPodTemplate,
            long applicationSeed) {}

    private static final ObjectMapper MAPPER =
            new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private String agentId;
    private String applicationId;
    @Deprecated private String image;
    @Deprecated private String imagePullPolicy;
    private String agentConfigSecretRef;
    private String agentConfigSecretRefChecksum;
    private String codeArchiveId;
    private Resources resources;
    private String options;
    @JsonIgnore private Options parsedOptions;

    @SneakyThrows
    private synchronized Options parseOptions() {
        if (parsedOptions == null) {
            if (options != null) {
                parsedOptions = MAPPER.readValue(options, Options.class);
            }
        }
        return parsedOptions;
    }

    @SneakyThrows
    public void serializeAndSetOptions(Options options) {
        this.options = MAPPER.writeValueAsString(options);
    }

    @JsonIgnore
    public List<Disk> getDisks() {
        final Options options = parseOptions();
        if (options == null) {
            return List.of();
        }
        return options.disks();
    }

    @JsonIgnore
    public boolean isAutoUpgradeRuntimeImage() {
        final Options options = parseOptions();
        if (options == null) {
            return false;
        }
        return options.autoUpgradeRuntimeImage();
    }

    @JsonIgnore
    public boolean isAutoUpgradeRuntimeImagePullPolicy() {
        final Options options = parseOptions();
        if (options == null) {
            return false;
        }
        return options.autoUpgradeRuntimeImagePullPolicy();
    }

    @JsonIgnore
    public boolean isAutoUpgradeAgentPodTemplate() {
        final Options options = parseOptions();
        if (options == null) {
            return false;
        }
        return options.autoUpgradeAgentPodTemplate();
    }

    @JsonIgnore
    public boolean isAutoUpgradeAgentResources() {
        final Options options = parseOptions();
        if (options == null) {
            return false;
        }
        return options.autoUpgradeAgentResources();
    }

    @JsonIgnore
    public long getApplicationSeed() {
        final Options options = parseOptions();
        if (options == null) {
            return 0L;
        }
        return options.applicationSeed();
    }
}
