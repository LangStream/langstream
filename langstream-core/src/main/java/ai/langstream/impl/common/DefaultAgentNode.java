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
package ai.langstream.impl.common;

import ai.langstream.api.model.DiskSpec;
import ai.langstream.api.model.ErrorsSpec;
import ai.langstream.api.model.ResourcesSpec;
import ai.langstream.api.runtime.AgentNode;
import ai.langstream.api.runtime.ComponentType;
import ai.langstream.api.runtime.ConnectionImplementation;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class DefaultAgentNode implements AgentNode {
    private final String id;
    private String agentType;
    private final ComponentType componentType;
    private Map<String, Object> configuration;
    private final Object customMetadata;

    private final ResourcesSpec resourcesSpec;
    private final Map<String, DiskSpec> disks;
    private final ErrorsSpec errorsSpec;

    private final ConnectionImplementation inputConnectionImplementation;
    private ConnectionImplementation outputConnectionImplementation;
    private final boolean composable;

    DefaultAgentNode(
            String id,
            String agentType,
            ComponentType componentType,
            Map<String, Object> configuration,
            boolean composable,
            Object runtimeMetadata,
            ConnectionImplementation inputConnectionImplementation,
            ConnectionImplementation outputConnectionImplementation,
            ResourcesSpec resourcesSpec,
            ErrorsSpec errorsSpec,
            Map<String, DiskSpec> disks) {
        this.agentType = agentType;
        this.composable = composable;
        this.id = id;
        this.componentType = componentType;
        this.configuration = configuration;
        this.customMetadata = runtimeMetadata;
        this.inputConnectionImplementation = inputConnectionImplementation;
        this.outputConnectionImplementation = outputConnectionImplementation;
        this.resourcesSpec = resourcesSpec != null ? resourcesSpec : ResourcesSpec.DEFAULT;
        this.errorsSpec = errorsSpec != null ? errorsSpec : ErrorsSpec.DEFAULT;
        this.disks = disks != null ? new HashMap<>(disks) : new HashMap<>();
    }

    public <T> T getCustomMetadata() {
        return (T) customMetadata;
    }

    public void overrideConfigurationAfterMerge(
            String agentType,
            Map<String, Object> newConfiguration,
            ConnectionImplementation newOutput,
            Map<String, DiskSpec> additionalDisks) {
        this.agentType = agentType;
        this.configuration = new HashMap<>(newConfiguration);
        this.outputConnectionImplementation = newOutput;
        if (additionalDisks != null) {
            this.disks.putAll(additionalDisks);
        }
    }

    @Override
    public ConnectionImplementation getInputConnectionImplementation() {
        return inputConnectionImplementation;
    }

    @Override
    public ConnectionImplementation getOutputConnectionImplementation() {
        return outputConnectionImplementation;
    }

    @Override
    public ResourcesSpec getResources() {
        return resourcesSpec;
    }
}
