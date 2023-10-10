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

import ai.langstream.api.doc.AgentConfigurationModel;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.deployer.k8s.util.SerializationUtil;
import ai.langstream.impl.noop.NoOpComputeClusterRuntimeProvider;
import java.util.Map;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class KafkaConnectAgentsProviderTest {
    @Test
    @SneakyThrows
    public void testValidationSource() {
        validate(
                """
                topics:
                - name: in
                - name: out
                pipeline:
                  - name: "my-source"
                    type: "source"
                    input: in
                    output: out
                    configuration:
                      connector.class: "io.confluent.connect.s3.S3SourceConnector"
                """,
                null);

        validate(
                """
                topics:
                - name: in
                - name: out
                pipeline:
                  - name: "my-source"
                    input: in
                    output: out
                    type: "source"
                    configuration: {}
                """,
                "Found error on agent configuration (agent: 'my-source', type: 'source'). Property 'connector.class' is required");

        validate(
                """
                topics:
                - name: in
                - name: out
                pipeline:
                  - name: "my-source"
                    type: "source"
                    input: in
                    output: out
                    configuration:
                      connector.class: "io.confluent.connect.s3.S3SourceConnector"
                      whatever.config:
                        inner: "value"
                """,
                null);
    }

    @Test
    @SneakyThrows
    public void testValidationSink() {
        validate(
                """
                topics:
                - name: in
                - name: out
                pipeline:
                  - name: "my-source"
                    type: "sink"
                    input: in
                    output: out
                    configuration:
                      connector.class: "io.confluent.connect.s3.S3SourceConnector"
                """,
                null);

        validate(
                """
                topics:
                - name: in
                - name: out
                pipeline:
                  - name: "my-source"
                    input: in
                    output: out
                    type: "sink"
                    configuration: {}
                """,
                "Found error on agent configuration (agent: 'my-source', type: 'sink'). Property 'connector.class' is required");

        validate(
                """
                topics:
                - name: in
                - name: out
                pipeline:
                  - name: "my-source"
                    type: "sink"
                    input: in
                    output: out
                    configuration:
                      connector.class: "io.confluent.connect.s3.S3SourceConnector"
                      whatever.config:
                        inner: "value"
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
                                "sink", new NoOpComputeClusterRuntimeProvider.NoOpClusterRuntime())
                        .generateSupportedTypesDocumentation();

        Assertions.assertEquals(
                """
                        {
                          "sink" : {
                            "name" : "Kafka Connect Sink agent",
                            "description" : "Run any Kafka Connect Sink.\\n    All the configuration properties are passed to the Kafka Connect Sink.",
                            "properties" : {
                              "connector.class" : {
                                "description" : "Java main class for the Kafka Sink connector.",
                                "required" : true,
                                "type" : "string"
                              }
                            }
                          },
                          "source" : {
                            "name" : "Kafka Connect Source agent",
                            "description" : "Run any Kafka Connect Source.\\n    All the configuration properties are passed to the Kafka Connect Source.",
                            "properties" : {
                              "connector.class" : {
                                "description" : "Java main class for the Kafka Source connector.",
                                "required" : true,
                                "type" : "string"
                              }
                            }
                          }
                        }""",
                SerializationUtil.prettyPrintJson(model));
    }
}
