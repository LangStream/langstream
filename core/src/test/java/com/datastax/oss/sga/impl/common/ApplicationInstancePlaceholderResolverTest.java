package com.datastax.oss.sga.impl.common;

import com.datastax.oss.sga.api.model.ApplicationInstance;
import com.datastax.oss.sga.api.model.Resource;
import com.datastax.oss.sga.impl.parser.ModelBuilder;
import com.samskivert.mustache.MustacheException;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ApplicationInstancePlaceholderResolverTest {

    @Test
    void testAvailablePlaceholders() throws Exception {

        ApplicationInstance applicationInstance = ModelBuilder
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
                                            webServiceUrl: http://mypulsar.localhost:8080
                                    globals:
                                        another-url: another-value
                                        open-api-url: http://myurl.localhost:8080/endpoint
                                """));

        final Map<String, Object> context = ApplicationInstancePlaceholderResolver.createContext(applicationInstance);
        Assertions.assertEquals("my-access-key",
                ApplicationInstancePlaceholderResolver.resolveValue(context, "{{secrets.openai-credentials.accessKey}}"));
        Assertions.assertEquals("http://mypulsar.localhost:8080",
                ApplicationInstancePlaceholderResolver.resolveValue(context, "{{cluster.configuration.webServiceUrl}}"));
        Assertions.assertEquals("http://myurl.localhost:8080/endpoint",
                ApplicationInstancePlaceholderResolver.resolveValue(context, "{{globals.open-api-url}}"));
    }

    @Test
    void testResolveSecretsInConfiguration() throws Exception {
        ApplicationInstance applicationInstance = ModelBuilder
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

        final ApplicationInstance resolved =
                ApplicationInstancePlaceholderResolver.resolvePlaceholders(applicationInstance);
        final Resource resource = resolved.getResources().get("openai-azure");
        Assertions.assertEquals("my-access-key", resource.configuration().get("credentials"));
        Assertions.assertEquals("http://myurl.localhost:8080/endpoint", resource.configuration().get("url"));
    }

    @Test
    void testErrorOnNotFound() throws Exception {
        ApplicationInstance applicationInstance = ModelBuilder
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
            ApplicationInstancePlaceholderResolver.resolvePlaceholders(applicationInstance);
        });
    }
}