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

import static org.junit.jupiter.api.Assertions.*;

import ai.langstream.api.doc.AgentConfigurationModel;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.deployer.k8s.util.SerializationUtil;
import ai.langstream.impl.noop.NoOpComputeClusterRuntimeProvider;
import java.util.Map;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class KubernetesGenAIToolKitFunctionAgentProviderTest {

    @Test
    @SneakyThrows
    public void testValidationDropFields() {
        validate(
                """
                topics: []
                pipeline:
                  - name: "drop-my-field"
                    type: "drop-fields"
                    configuration:
                      a-field: "val"
                """,
                "Found error on agent configuration (agent: 'drop-my-field', type: 'drop-fields'). Property 'a-field' is unknown");
        validate(
                """
                topics: []
                pipeline:
                  - name: "drop-my-field"
                    type: "drop-fields"
                    configuration: {}
                """,
                "Found error on agent configuration (agent: 'drop-my-field', type: 'drop-fields'). Property 'fields' is required");
        validate(
                """
                topics: []
                pipeline:
                  - name: "drop-my-field"
                    type: "drop-fields"
                    configuration:
                        fields: {}
                """,
                "Found error on agent configuration (agent: 'drop-my-field', type: 'drop-fields'). Property 'fields' has a wrong data type. Expected type: java.util.ArrayList");
        validate(
                """
                topics: []
                pipeline:
                  - name: "drop-my-field"
                    type: "drop-fields"
                    configuration:
                      fields:
                      - "a"
                      part: value
                """,
                null);
    }

    @Test
    @SneakyThrows
    public void testValidationAiChatCompletions() {
        validate(
                """
                topics: []
                pipeline:
                  - name: "chat"
                    type: "ai-chat-completions"
                    configuration:
                        model: my-model
                        messages:
                         - role: system
                           content: "Hello"
                """,
                "Found error on agent configuration (agent: 'chat', type: 'ai-chat-completions'). No ai service resource found in application configuration. One of vertex-configuration, hugging-face-configuration, open-ai-configuration, bedrock-configuration must be defined.");

        AgentValidationTestUtil.validate(
                """
                topics: []
                pipeline:
                  - name: "chat"
                    type: "ai-chat-completions"
                    configuration:
                        model: my-model
                        messages:
                         - role: system
                           content: "Hello"
                """,
                """
                        configuration:
                            resources:
                                - type: "open-ai-configuration"
                                  name: "OpenAI Azure configuration"
                                  configuration:
                                    access-key: "yy"
                        """,
                null);
    }

    private void validate(String pipeline, String expectErrMessage) throws Exception {
        AgentValidationTestUtil.validate(pipeline, expectErrMessage);
    }

    @Test
    @SneakyThrows
    public void testDocumentation() {
        final Map<String, AgentConfigurationModel> model =
                new PluginsRegistry()
                        .lookupAgentImplementation(
                                "drop-fields",
                                new NoOpComputeClusterRuntimeProvider.NoOpClusterRuntime())
                        .generateSupportedTypesDocumentation();

        Assertions.assertEquals(
                """

""",
                SerializationUtil.prettyPrintJson(model));
    }
}
