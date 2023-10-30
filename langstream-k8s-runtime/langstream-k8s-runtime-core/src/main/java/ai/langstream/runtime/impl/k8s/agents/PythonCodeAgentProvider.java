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
package ai.langstream.runtime.impl.k8s.agents;

import ai.langstream.api.doc.AgentConfig;
import ai.langstream.api.doc.ConfigProperty;
import ai.langstream.api.model.AgentConfiguration;
import ai.langstream.api.runtime.ComponentType;
import ai.langstream.impl.agents.AbstractComposableAgentProvider;
import ai.langstream.runtime.impl.k8s.KubernetesClusterRuntime;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/** Implements support for custom Agents written in Python. */
@Slf4j
public class PythonCodeAgentProvider extends AbstractComposableAgentProvider {

    public PythonCodeAgentProvider() {
        super(
                Set.of(
                        "python-source",
                        "python-sink",
                        "python-processor",
                        "python-function",
                        "python-service"),
                List.of(KubernetesClusterRuntime.CLUSTER_TYPE, "none"));
    }

    @Override
    protected final ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        return switch (agentConfiguration.getType()) {
            case "python-service" -> ComponentType.SERVICE;
            case "python-source" -> ComponentType.SOURCE;
            case "python-sink" -> ComponentType.SINK;
            case "python-processor", "python-function" -> ComponentType.PROCESSOR;
            default -> throw new IllegalArgumentException(
                    "Unsupported agent type: " + agentConfiguration.getType());
        };
    }

    @Override
    protected Class getAgentConfigModelClass(String type) {
        return switch (type) {
            case "python-source" -> PythonSourceConfig.class;
            case "python-service" -> PythonServiceConfig.class;
            case "python-sink" -> PythonSinkConfig.class;
            case "python-processor", "python-function" -> PythonProcessorConfig.class;
            default -> throw new IllegalArgumentException("Unsupported agent type: " + type);
        };
    }

    @Override
    protected boolean isAgentConfigModelAllowUnknownProperties(String type) {
        return true;
    }

    @AgentConfig(
            name = "Python custom source",
            description =
                    """
                    Run a your own Python source.
                    All the configuration properties are available in the class init method.
                    """)
    public static class PythonSourceConfig extends PythonConfig {}

    @AgentConfig(
            name = "Python custom service",
            description =
                    """
                    Run a your own Python service.
                    All the configuration properties are available in the class init method.
                    """)
    public static class PythonServiceConfig extends PythonConfig {}

    @AgentConfig(
            name = "Python custom sink",
            description =
                    """
                    Run a your own Python sink.
                    All the configuration properties are available in the class init method.
                    """)
    public static class PythonSinkConfig extends PythonConfig {}

    @AgentConfig(
            name = "Python custom processor",
            description =
                    """
                    Run a your own Python processor.
                    All the configuration properties are available the class init method.
                    """)
    public static class PythonProcessorConfig extends PythonConfig {}

    public static class PythonConfig {
        @ConfigProperty(
                description =
                        """
                                Python class name to instantiate. This class must be present in the application's "python" files.
                                        """,
                required = true)
        private String className;
    }
}
