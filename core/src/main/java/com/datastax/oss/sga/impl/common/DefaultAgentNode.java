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
package com.datastax.oss.sga.impl.common;

import com.datastax.oss.sga.api.model.ErrorsSpec;
import com.datastax.oss.sga.api.model.ResourcesSpec;
import com.datastax.oss.sga.api.runtime.AgentNode;
import com.datastax.oss.sga.api.runtime.ComponentType;
import com.datastax.oss.sga.api.runtime.ConnectionImplementation;
import lombok.Getter;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

@Getter
@ToString
public class DefaultAgentNode implements AgentNode {
    private final String id;
    private String agentType;
    private final ComponentType componentType;
    private Map<String, Object> configuration;
    private final Object customMetadata;

    private final ResourcesSpec resourcesSpec;
    private final ErrorsSpec errorsSpec;

    private final ConnectionImplementation inputConnectionImplementation;
    private ConnectionImplementation outputConnectionImplementation;
    private final boolean composable;

    DefaultAgentNode(String id, String agentType,
                     ComponentType componentType,
                     Map<String, Object> configuration,
                     boolean composable, Object runtimeMetadata,
                            ConnectionImplementation inputConnectionImplementation,
                            ConnectionImplementation outputConnectionImplementation,
                            ResourcesSpec resourcesSpec,
                            ErrorsSpec errorsSpec) {
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
    }

    public <T> T getCustomMetadata() {
        return (T) customMetadata;
    }

    public void overrideConfigurationAfterMerge(String agentType, Map<String, Object> newConfiguration, ConnectionImplementation newOutput) {
        this.agentType = agentType;
        this.configuration = new HashMap<>(newConfiguration);
        this.outputConnectionImplementation = newOutput;
    }

    @Override
    public ConnectionImplementation getInputConnectionImplementation() {
        return inputConnectionImplementation;
    }

    @Override
    public ConnectionImplementation getOutputConnectionImplementation() {
        return outputConnectionImplementation;
    }
}
