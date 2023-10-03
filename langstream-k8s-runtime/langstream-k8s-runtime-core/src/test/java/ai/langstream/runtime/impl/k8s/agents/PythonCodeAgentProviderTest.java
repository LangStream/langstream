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

import lombok.SneakyThrows;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class PythonCodeAgentProviderTest {
    @ParameterizedTest
    @ValueSource(strings = {"python-source", "python-sink", "python-processor", "python-function"})
    @SneakyThrows
    public void testValidation(String type) {
        validate(
                """
                topics: []
                pipeline:
                  - name: "python1"
                    type: "%s"
                    configuration:
                      a-field: "val"
                """
                        .formatted(type),
                "Found error on agent configuration (agent: 'python1', type: '%s'). Property 'className' is required"
                        .formatted(type));
        validate(
                """
                topics: []
                pipeline:
                  - name: "python1"
                    type: "%s"
                    configuration: {}
                """
                        .formatted(type),
                "Found error on agent configuration (agent: 'python1', type: '%s'). Property 'className' is required"
                        .formatted(type));
        validate(
                """
                topics: []
                pipeline:
                  - name: "python1"
                    type: "%s"
                    configuration:
                      className: my.class
                      other: true
                """
                        .formatted(type),
                null);
    }

    private void validate(String pipeline, String expectErrMessage) throws Exception {
        AgentValidationTestUtil.validate(pipeline, expectErrMessage);
    }
}
