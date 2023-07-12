package com.datastax.oss.sga.impl.common;

import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.Resource;
import com.datastax.oss.sga.impl.parser.ModelBuilder;
import com.samskivert.mustache.MustacheException;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ApplicationPlaceholderResolverTest {

    @Test
    void testAvailablePlaceholders() throws Exception {

        Application applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of(
                        "secrets.yaml", """
                                secrets:
                                    - name: "OpenAI Azure credentials"
                                      id: "openai-credentials"
                                      data:
                                        accessKey: "my-access-key"
                                """,
                        "instance.yaml", """
                                instance:
                                    streamingCluster:
                                        type: pulsar
                                        configuration:
                                            admin: 
                                                serviceUrl: http://mypulsar.localhost:8080
                                    globals:
                                        another-url: another-value
                                        open-api-url: http://myurl.localhost:8080/endpoint
                                """));

        final Map<String, Object> context = ApplicationPlaceholderResolver.createContext(applicationInstance);
        Assertions.assertEquals("my-access-key",
                ApplicationPlaceholderResolver.resolveValue(context,
                        "{{secrets.openai-credentials.accessKey}}"));
        Assertions.assertEquals("http://mypulsar.localhost:8080",
                ApplicationPlaceholderResolver.resolveValue(context,
                        "{{cluster.configuration.admin.serviceUrl}}"));
        Assertions.assertEquals("http://myurl.localhost:8080/endpoint",
                ApplicationPlaceholderResolver.resolveValue(context, "{{globals.open-api-url}}"));
    }

    @Test
    void testResolveSecretsInConfiguration() throws Exception {
        Application applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of("configuration.yaml",
                        """
                                configuration:
                                    resources:
                                        - type: "openai-azure-config"
                                          name: "OpenAI Azure configuration"
                                          id: "openai-azure"
                                          configuration:
                                            credentials: "{{secrets.openai-credentials.accessKey}}"
                                            url: "{{globals.open-api-url}}"
                                    
                                """,
                        "secrets.yaml", """
                                secrets:
                                    - name: "OpenAI Azure credentials"
                                      id: "openai-credentials"
                                      data:
                                        accessKey: "my-access-key"
                                """,
                        "instance.yaml", """
                                instance:
                                    globals:
                                        another-url: another-value
                                        open-api-url: http://myurl.localhost:8080/endpoint
                                """));

        final Application resolved =
                ApplicationPlaceholderResolver.resolvePlaceholders(applicationInstance);
        final Resource resource = resolved.getResources().get("openai-azure");
        Assertions.assertEquals("my-access-key", resource.configuration().get("credentials"));
        Assertions.assertEquals("http://myurl.localhost:8080/endpoint", resource.configuration().get("url"));
    }

    @Test
    void testResolveInAgentConfiguration() throws Exception {
        Application applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of("module1.yaml",
                        """
                                module: "module-1"
                                id: "pipeline-1"
                                topics:
                                    - name: "input-topic"
                                pipeline:
                                  - name: "sink1"
                                    id: "sink1"
                                    type: "generic-pulsar-sink"
                                    input: "input-topic"
                                    configuration:
                                      sinkType: "some-sink-type-on-your-cluster"
                                      access-key: "{{ secrets.ak.value }}"
                                """,
                        "secrets.yaml", """
                                secrets:
                                    - name: "OpenAI Azure credentials"
                                      id: "ak"
                                      data:
                                        value: "my-access-key"
                                """));

        final Application resolved =
                ApplicationPlaceholderResolver.resolvePlaceholders(applicationInstance);
        Assertions.assertEquals("my-access-key",
                resolved.getModule("module-1").getPipelines().values().iterator().next().getAgents().get("sink1")
                        .getConfiguration()
                        .get("access-key"));
    }

    @Test
    void testErrorOnNotFound() throws Exception {
        Application applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of("configuration.yaml",
                        """
                                configuration:
                                    resources:
                                        - type: "openai-azure-config"
                                          name: "OpenAI Azure configuration"
                                          id: "openai-azure"
                                          configuration:
                                            credentials: "{{secrets.openai-credentials.invalid}}"
                                    
                                """));
        Assertions.assertThrows(MustacheException.Context.class, () -> {
            ApplicationPlaceholderResolver.resolvePlaceholders(applicationInstance);
        });
    }

    @Test
    void testKeepStruct() throws Exception {
        Application applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of(
                        "instance.yaml", """
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
                                """));

        final Application resolved =
                ApplicationPlaceholderResolver.resolvePlaceholders(applicationInstance);
        final Map<String, Object> configuration = resolved.getInstance().streamingCluster()
                .configuration();
        Assertions.assertTrue(configuration.get("rootObject") instanceof Map);
        Assertions.assertTrue(configuration.get("rootArray") instanceof java.util.List);
        Assertions.assertTrue(configuration.get("myvalue") instanceof String);
    }


    @Test
    void testEscapeMustache() throws Exception {
        Assertions.assertEquals("{{ do not resolve }} resolved",
                ApplicationPlaceholderResolver.resolveValue(Map.of("test", "resolved"),
                        "{{% do not resolve }} {{ test }}"));
    }
}