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

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.langstream.api.model.AgentConfiguration;
import ai.langstream.api.model.Application;
import ai.langstream.api.model.Resource;
import ai.langstream.impl.parser.ModelBuilder;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ApplicationPlaceholderResolverTest {

    @Test
    void testAvailablePlaceholders() throws Exception {

        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(),
                                """
                                instance:
                                    streamingCluster:
                                        type: pulsar
                                        configuration:
                                            admin:
                                                serviceUrl: http://mypulsar.localhost:8080
                                    globals:
                                        another-url: another-value
                                        open-api-url: http://myurl.localhost:8080/endpoint
                                """,
                                """

                                secrets:
                                    - name: "OpenAI Azure credentials"
                                      id: "openai-credentials"
                                      data:
                                        accessKey: "my-access-key"
                                """)
                        .getApplication();

        final Map<String, Object> context =
                ApplicationPlaceholderResolver.createContext(applicationInstance);
        assertEquals(
                "my-access-key",
                ApplicationPlaceholderResolver.resolveSingleValue(
                        context, "${secrets.openai-credentials.accessKey}"));
        assertEquals(
                "http://myurl.localhost:8080/endpoint",
                ApplicationPlaceholderResolver.resolveSingleValue(
                        context, "${globals.open-api-url}"));
    }

    @Test
    void testResolveSecretsInConfiguration() throws Exception {
        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
                                        "configuration.yaml",
                                        """
                                        configuration:
                                            resources:
                                                - type: "openai-azure-config"
                                                  name: "OpenAI Azure configuration"
                                                  id: "openai-azure"
                                                  configuration:
                                                    credentials: "${secrets.openai-credentials.accessKey}"
                                                    url: "${globals.open-api-url}"

                                        """),
                                """
                                instance:
                                    globals:
                                        another-url: another-value
                                        open-api-url: http://myurl.localhost:8080/endpoint
                                """,
                                """
                                secrets:
                                    - name: "OpenAI Azure credentials"
                                      id: "openai-credentials"
                                      data:
                                        accessKey: "my-access-key"
                                        """)
                        .getApplication();

        final Application resolved =
                ApplicationPlaceholderResolver.resolvePlaceholders(applicationInstance);
        final Resource resource = resolved.getResources().get("openai-azure");
        assertEquals("my-access-key", resource.configuration().get("credentials"));
        assertEquals("http://myurl.localhost:8080/endpoint", resource.configuration().get("url"));
    }

    @Test
    void testResolveInAgentConfiguration() throws Exception {
        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
                                        "module1.yaml",
                                        """
                                        module: "module-1"
                                        id: "pipeline-1"
                                        topics:
                                            - name: "${globals.input-topic}"
                                            - name: "${globals.output-topic}"
                                            - name: "${globals.stream-response-topic}"
                                        pipeline:
                                          - name: "agent1"
                                            id: "agent1"
                                            type: "ai-chat-completions"
                                            input: "${globals.input-topic}"
                                            output: "${globals.output-topic}"
                                            configuration:
                                              stream-to-topic: "${globals.stream-response-topic}"
                                              sinkType: "some-sink-type-on-your-cluster"
                                              access-key: "${secrets.ak.value}"
                                              int-value: 42
                                        """),
                                """
                                instance:
                                    globals:
                                        input-topic: my-input-topic
                                        output-topic: my-output-topic
                                        stream-response-topic: my-stream-topic
                                """,
                                """
                                secrets:
                                    - name: "OpenAI Azure credentials"
                                      id: "ak"
                                      data:
                                        value: "my-access-key"
                                """)
                        .getApplication();

        final Application resolved =
                ApplicationPlaceholderResolver.resolvePlaceholders(applicationInstance);
        AgentConfiguration agentConfiguration =
                resolved
                        .getModule("module-1")
                        .getPipelines()
                        .values()
                        .iterator()
                        .next()
                        .getAgents()
                        .stream()
                        .filter(agent -> agent.getId().equals("agent1"))
                        .findFirst()
                        .orElseThrow();
        assertEquals("my-access-key", agentConfiguration.getConfiguration().get("access-key"));
        assertEquals(42, agentConfiguration.getConfiguration().get("int-value"));
        assertEquals("my-input-topic", agentConfiguration.getInput().definition());
        assertEquals("my-output-topic", agentConfiguration.getOutput().definition());
        assertEquals(
                "my-stream-topic", agentConfiguration.getConfiguration().get("stream-to-topic"));
        assertEquals(
                "my-stream-topic",
                resolved.getModule("module-1").getTopics().get("my-stream-topic").getName());
        assertEquals(
                "my-input-topic",
                resolved.getModule("module-1").getTopics().get("my-input-topic").getName());
        assertEquals(
                "my-output-topic",
                resolved.getModule("module-1").getTopics().get("my-output-topic").getName());
    }

    @Test
    void testErrorOnNotFound() throws Exception {
        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
                                        "configuration.yaml",
                                        """
                                configuration:
                                    resources:
                                        - type: "openai-azure-config"
                                          name: "OpenAI Azure configuration"
                                          id: "openai-azure"
                                          configuration:
                                            credentials: "${secrets.openai-credentials.invalid}"

                                """),
                                null,
                                null)
                        .getApplication();
        IllegalArgumentException illegalArgumentException =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                ApplicationPlaceholderResolver.resolvePlaceholders(
                                        applicationInstance));
        assertEquals(
                "Cannot resolve reference secrets.openai-credentials.invalid",
                illegalArgumentException.getMessage());
    }

    @Test
    void testKeepStruct() throws Exception {
        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(),
                                """
                                instance:
                                    streamingCluster:
                                        type: pulsar
                                        configuration:
                                            rootObject:
                                                nestedObject: "value"
                                            rootArray:
                                                - nestedObject: "value"
                                                - nestedObject: "value"
                                            myvalue: "thevalue"
                                """,
                                null)
                        .getApplication();

        final Application resolved =
                ApplicationPlaceholderResolver.resolvePlaceholders(applicationInstance);
        final Map<String, Object> configuration =
                resolved.getInstance().streamingCluster().configuration();
        Assertions.assertTrue(configuration.get("rootObject") instanceof Map);
        Assertions.assertTrue(configuration.get("rootArray") instanceof java.util.List);
        Assertions.assertTrue(configuration.get("myvalue") instanceof String);
    }

    @Test
    void testResolveTopicsInGateway() throws Exception {
        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
                                        "module1.yaml",
                                        """
                                        module: "module-1"
                                        id: "pipeline-1"
                                        topics:
                                            - name: "${globals.input-topic}"
                                            - name: "${globals.output-topic}"
                                            - name: "${globals.stream-response-topic}"
                                        pipeline:
                                          - name: "agent1"
                                            id: "agent1"
                                            type: "ai-chat-completions"
                                            input: "${globals.input-topic}"
                                            output: "${globals.output-topic}"
                                        """,
                                        "gateways.yaml",
                                        """
                                        gateways:
                                          - id: produce
                                            type: produce
                                            topic: "${globals.input-topic}"
                                            events-topic: "${globals.stream-response-topic}"
                                            produce-options: {}
                                          - id: consume
                                            type: consume
                                            topic: "${globals.input-topic}"
                                            events-topic: "${globals.stream-response-topic}"
                                            consume-options: {}
                                        """),
                                """
                                instance:
                                    globals:
                                        input-topic: my-input-topic
                                        output-topic: my-output-topic
                                        stream-response-topic: my-stream-topic
                                """,
                                null)
                        .getApplication();

        final Application resolved =
                ApplicationPlaceholderResolver.resolvePlaceholders(applicationInstance);
        assertEquals("my-input-topic", resolved.getGateways().gateways().get(0).getTopic());
        assertEquals("my-stream-topic", resolved.getGateways().gateways().get(0).getEventsTopic());
        assertEquals("my-input-topic", resolved.getGateways().gateways().get(1).getTopic());
        assertEquals("my-stream-topic", resolved.getGateways().gateways().get(1).getEventsTopic());
    }

    @Test
    void testResolveVariablesInAssets() throws Exception {
        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
                                        "module1.yaml",
                                        """
                                        module: "module-1"
                                        id: "pipeline-1"
                                        assets:
                                            - name: "by asset"
                                              asset-type: "some-type"
                                              config:
                                                 some-value: "${globals.table-name}"
                                        pipeline:
                                          - name: "agent1"
                                            id: "agent1"
                                            type: "identity"
                                        """),
                                """
                                instance:
                                    globals:
                                        table-name: my-table
                                """,
                                null)
                        .getApplication();

        final Application resolved =
                ApplicationPlaceholderResolver.resolvePlaceholders(applicationInstance);
        assertEquals(
                "my-table",
                resolved.getModule("module-1").getAssets().get(0).getConfig().get("some-value"));
    }

    @Test
    void testResolveAsString() {
        assertEquals("test", ApplicationPlaceholderResolver.resolveValueAsString(Map.of(), "test"));
        assertEquals(
                "xxx",
                ApplicationPlaceholderResolver.resolveValueAsString(
                        Map.of("globals", Map.of("foo", Map.of("bar", "xxx"))),
                        "${globals.foo.bar}"));
    }

    @Test
    void testResolve() {
        Map<String, Object> context =
                Map.of(
                        "globals",
                        Map.of(
                                "foo",
                                Map.of(
                                        "bar",
                                        "xxx",
                                        "number",
                                        123,
                                        "list",
                                        List.of(1, 2),
                                        "map",
                                        Map.of("one", 1, "two", 2))));
        assertEquals(
                "xxx",
                ApplicationPlaceholderResolver.resolveSingleValue(context, "${globals.foo.bar}"));
        assertEquals(
                123,
                ApplicationPlaceholderResolver.resolveSingleValue(
                        context, "${globals.foo.number}"));
        assertEquals(
                List.of(1, 2),
                ApplicationPlaceholderResolver.resolveSingleValue(context, "${globals.foo.list}"));

        // some spaces
        assertEquals(
                123,
                ApplicationPlaceholderResolver.resolveSingleValue(
                        context, "${  globals.foo.number  }"));

        // simple concat
        assertEquals(
                "123-xxx",
                ApplicationPlaceholderResolver.resolveSingleValue(
                        context, "${  globals.foo.number  }-${  globals.foo.bar  }"));

        // using a list, but in a string context
        assertEquals(
                "123-[1,2]",
                ApplicationPlaceholderResolver.resolveSingleValue(
                        context, "${  globals.foo.number  }-${  globals.foo.list  }"));

        // using a map, but in a string context
        assertEquals(
                "123-{\"one\":1,\"two\":2}",
                ApplicationPlaceholderResolver.resolveSingleValue(
                        context, "${  globals.foo.number  }-${  globals.foo.map  }"));
    }

    @Test
    void testResolveCompatibilityTripleBraces() {
        Map<String, Object> context =
                Map.of(
                        "globals",
                        Map.of(
                                "foo",
                                Map.of(
                                        "bar",
                                        "xxx",
                                        "number",
                                        123,
                                        "list",
                                        List.of(1, 2),
                                        "map",
                                        Map.of("one", 1, "two", 2))));
        assertEquals(
                "xxx",
                ApplicationPlaceholderResolver.resolveSingleValue(
                        context, "{{{globals.foo.bar}}}"));
        assertEquals(
                "123", // this is a string !
                ApplicationPlaceholderResolver.resolveSingleValue(
                        context, "{{{globals.foo.number}}}"));
        assertEquals(
                "[1,2]", // this is a string !
                ApplicationPlaceholderResolver.resolveSingleValue(
                        context, "{{{globals.foo.list}}}"));

        // some spaces
        assertEquals(
                "123", // this is a string !
                ApplicationPlaceholderResolver.resolveSingleValue(
                        context, "{{{  globals.foo.number  }}}"));

        // simple concat
        assertEquals(
                "123-xxx",
                ApplicationPlaceholderResolver.resolveSingleValue(
                        context, "{{{  globals.foo.number  }}}-{{{  globals.foo.bar  }}}"));

        // using a list, but in a string context
        assertEquals(
                "123-[1,2]",
                ApplicationPlaceholderResolver.resolveSingleValue(
                        context, "{{{ globals.foo.number  }}}-{{{  globals.foo.list  }}}"));

        // using a map, but in a string context
        assertEquals(
                "123-{\"one\":1,\"two\":2}",
                ApplicationPlaceholderResolver.resolveSingleValue(
                        context, "{{{  globals.foo.number  }}}-{{{  globals.foo.map  }}}"));
    }

    @Test
    void testResolveCompatibilityDoubleBraces() {
        Map<String, Object> context =
                Map.of(
                        "globals",
                        Map.of(
                                "foo",
                                Map.of(
                                        "bar",
                                        "xxx",
                                        "number",
                                        123,
                                        "list",
                                        List.of(1, 2),
                                        "map",
                                        Map.of("one", 1, "two", 2))));
        assertEquals(
                "xxx",
                ApplicationPlaceholderResolver.resolveSingleValue(context, "{{globals.foo.bar}}"));
        assertEquals(
                "123", // this is a string !
                ApplicationPlaceholderResolver.resolveSingleValue(
                        context, "{{globals.foo.number}}"));
        assertEquals(
                "[1,2]", // this is a string !
                ApplicationPlaceholderResolver.resolveSingleValue(context, "{{globals.foo.list}}"));

        // some spaces
        assertEquals(
                "123", // this is a string !
                ApplicationPlaceholderResolver.resolveSingleValue(
                        context, "{{  globals.foo.number  }}"));

        // simple concat
        assertEquals(
                "123-xxx",
                ApplicationPlaceholderResolver.resolveSingleValue(
                        context, "{{  globals.foo.number  }}-{{  globals.foo.bar  }}"));

        // using a list, but in a string context
        assertEquals(
                "123-[1,2]",
                ApplicationPlaceholderResolver.resolveSingleValue(
                        context, "{{ globals.foo.number  }}-{{  globals.foo.list  }}"));

        // using a map, but in a string context
        assertEquals(
                "123-{\"one\":1,\"two\":2}",
                ApplicationPlaceholderResolver.resolveSingleValue(
                        context, "{{  globals.foo.number  }}-{{  globals.foo.map  }}"));
    }

    @Test
    void testDontBreakAMustacheValue() {
        Map<String, Object> context =
                Map.of(
                        "something",
                        Map.of("foo", Map.of("bar", "xxx", "number", 123, "list", List.of(1, 2))),
                        "globals",
                        Map.of(
                                "foo",
                                Map.of(
                                        "bar",
                                        "xxx",
                                        "number",
                                        123,
                                        "list",
                                        List.of(1, 2),
                                        "map",
                                        Map.of("one", 1, "two", 2))));
        assertEquals(
                "{{something.foo.bar}}",
                ApplicationPlaceholderResolver.resolveSingleValue(
                        context, "{{something.foo.bar}}"));
    }
}
