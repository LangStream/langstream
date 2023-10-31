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
import ai.langstream.api.doc.ExtendedValidationType;
import ai.langstream.api.model.AgentConfiguration;
import ai.langstream.api.runtime.ComponentType;
import ai.langstream.impl.agents.AbstractComposableAgentProvider;
import ai.langstream.runtime.impl.k8s.KubernetesClusterRuntime;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HttpRequestAgentProvider extends AbstractComposableAgentProvider {

    private static final Set<String> SUPPORTED_AGENT_TYPES =
            Set.of("http-request", "langserve-invoke");

    public HttpRequestAgentProvider() {
        super(SUPPORTED_AGENT_TYPES, List.of(KubernetesClusterRuntime.CLUSTER_TYPE, "none"));
    }

    @Override
    protected ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        return ComponentType.PROCESSOR;
    }

    @Override
    protected Class getAgentConfigModelClass(String type) {
        return switch (type) {
            case "http-request" -> HttpRequestConfig.class;
            case "langserve-invoke" -> LangServeInvokeConfig.class;
            default -> throw new IllegalArgumentException("Unknown agent type: " + type);
        };
    }

    @AgentConfig(
            name = "Http Request",
            description =
                    """
                            Agent for enriching data with an HTTP request.
                            """)
    @Data
    public static class HttpRequestConfig {
        @ConfigProperty(
                description =
                        """
                                Url to send the request to. For adding query string parameters, use the `query-string` field.
                                            """,
                required = true)
        private String url;

        @ConfigProperty(
                description =
                        """
                                The field that will hold the results, it can be the same as "field" to override it.
                                            """,
                required = true)
        @JsonProperty("output-field")
        private String outputFieldName;

        @ConfigProperty(
                description =
                        """
                                Http method to use for the request.
                                            """,
                defaultValue = "GET")
        private String method;

        @ConfigProperty(
                description =
                        """
                                Headers to send with the request. You can use the Mustache syntax to inject value from the context.
                                            """)
        private Map<String, String> headers;

        @ConfigProperty(
                description =
                        """
                                Query string to append to the url. You can use the Mustache syntax to inject value from the context.
                                Note that the values will be automatically escaped.
                                            """)
        @JsonProperty("query-string")
        private Map<String, String> queryString;

        @ConfigProperty(
                description =
                        """
                                Body to send with the request. You can use the Mustache syntax to inject value from the context.
                                            """)
        private String body;

        @ConfigProperty(
                description =
                        """
                                Whether or not to follow redirects.
                                            """,
                defaultValue = "true")
        @JsonProperty("allow-redirects")
        private boolean allowRedirects;

        @ConfigProperty(
                description =
                        """
                                Whether or not to handle cookies during the redirects.
                                            """,
                defaultValue = "true")
        @JsonProperty("handle-cookies")
        private boolean handleCookies;
    }

    @AgentConfig(
            name = "Invoke LangServe",
            description =
                    """
                            Agent for invoking LangServe based applications
                            """)
    @Data
    public static class LangServeInvokeConfig {
        @ConfigProperty(
                description =
                        """
                                Url to send the request to. For adding query string parameters, use the `query-string` field.
                                            """,
                required = true)
        private String url;

        @ConfigProperty(
                description =
                        """
                                The field that will hold the results, it can be the same as "field" to override it.
                                            """,
                required = true,
                defaultValue = "value")
        @JsonProperty("output-field")
        private String outputFieldName = "value";

        @ConfigProperty(
                description =
                        """
                                Field in the response that will be used as the content of the record.
                                            """,
                required = false,
                defaultValue = "content")
        @JsonProperty("content-field")
        private String contentFieldName = "content";

        @ConfigProperty(
                description =
                        """
                                Field in the response that will be used as the content of the record.
                                            """)
        @JsonProperty("debug")
        private boolean debug;

        @ConfigProperty(
                description =
                        """
                                Http method to use for the request.
                                            """,
                defaultValue = "POST")
        private String method = "POST";

        @ConfigProperty(
                description =
                        """
                                Headers to send with the request. You can use the Mustache syntax to inject value from the context.
                                            """)
        private Map<String, String> headers;

        @ConfigProperty(
                description =
                        """
                                Whether or not to follow redirects.
                                            """,
                defaultValue = "true")
        @JsonProperty("allow-redirects")
        private boolean allowRedirects;

        @ConfigProperty(
                description =
                        """
                                Whether or not to handle cookies during the redirects.
                                            """,
                defaultValue = "true")
        @JsonProperty("handle-cookies")
        private boolean handleCookies;

        @ConfigProperty(
                description =
                        """
                        Fields of the generated records.
                                """)
        List<FieldConfiguration> fields;
    }

    @Data
    public static class FieldConfiguration {
        @ConfigProperty(
                description =
                        """
                        Name of the field like value.xx, key.xxx, properties.xxx
                                """,
                required = true)
        String name;

        @ConfigProperty(
                description =
                        """
                        Expression to compute the value of the field. This is a standard EL expression.
                                """,
                required = true,
                extendedValidationType = ExtendedValidationType.EL_EXPRESSION)
        String expression;
    }
}
