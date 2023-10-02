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
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Set;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/** Implements support for Text Processing Agents. */
@Slf4j
public class TextProcessingAgentsProvider extends AbstractComposableAgentProvider {

    protected static final String TEXT_EXTRACTOR = "text-extractor";
    protected static final String LANGUAGE_DETECTOR = "language-detector";
    protected static final String TEXT_SPLITTER = "text-splitter";
    protected static final String TEXT_NORMALISER = "text-normaliser";
    protected static final String DOCUMENT_TO_JSON = "document-to-json";
    private static final Set<String> SUPPORTED_AGENT_TYPES =
            Set.of(
                    TEXT_EXTRACTOR,
                    LANGUAGE_DETECTOR,
                    TEXT_SPLITTER,
                    TEXT_NORMALISER,
                    DOCUMENT_TO_JSON);

    public TextProcessingAgentsProvider() {
        super(SUPPORTED_AGENT_TYPES, List.of(KubernetesClusterRuntime.CLUSTER_TYPE, "none"));
    }

    @Override
    protected ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        return ComponentType.PROCESSOR;
    }

    @Override
    protected Class getAgentConfigModelClass(String type) {
        return switch (type) {
            case TEXT_EXTRACTOR -> TextExtractorConfig.class;
            case LANGUAGE_DETECTOR -> LanguageDetectorConfig.class;
            case TEXT_SPLITTER -> TextSplitterConfig.class;
            case TEXT_NORMALISER -> TextNormaliserConfig.class;
            case DOCUMENT_TO_JSON -> DocumentToJsonConfig.class;
            default -> throw new IllegalArgumentException("Unsupported agent type: " + type);
        };
    }

    @AgentConfig(
            name = "Text extractor",
            description =
                    """
            Extracts text content from different document formats like PDF, JSON, XML, ODF, HTML and many others.
            """)
    @Data
    public static class TextExtractorConfig {}

    @AgentConfig(
            name = "Language detector",
            description =
                    """
            Detect the language of a message’s data and limit further processing based on language codes.
            """)
    @Data
    public static class LanguageDetectorConfig {
        @ConfigProperty(
                description =
                        """
                        The name of the message header to write the language code to.
                                """,
                defaultValue = "language")
        private String property;

        @ConfigProperty(
                description =
                        """
                        Define a list of allowed language codes. If the message language is not in this list, the message is dropped.
                                """)
        private List<String> allowedLanguages;
    }

    @AgentConfig(
            name = "Text splitter",
            description = """
            Split message content in chunks.
            """)
    @Data
    public static class TextSplitterConfig {
        @ConfigProperty(
                description =
                        """
                        Splitter implementation to use. Currently supported: RecursiveCharacterTextSplitter.
                                """,
                defaultValue = "RecursiveCharacterTextSplitter")
        private String splitter_type;

        @ConfigProperty(
                description =
                        """
                        RecursiveCharacterTextSplitter splitter option. The separator to use for splitting.
                        Checkout https://github.com/knuddelsgmbh/jtokkit for more details.
                        """,
                defaultValue = "\"\\n\\n\", \"\\n\", \" \", \"\"")
        private List<String> separators;

        @ConfigProperty(
                description =
                        """
                        RecursiveCharacterTextSplitter splitter option. Whether or not to keep separators.
                        Checkout https://github.com/knuddelsgmbh/jtokkit for more details.
                                """,
                defaultValue = "false")
        private boolean keep_separator;

        @ConfigProperty(
                description =
                        """
                        RecursiveCharacterTextSplitter splitter option. Chunk size of each message.
                        Checkout https://github.com/knuddelsgmbh/jtokkit for more details.
                                """,
                defaultValue = "200")
        private int chunk_size;

        @ConfigProperty(
                description =
                        """
                        RecursiveCharacterTextSplitter splitter option. Chunk overlap of the previous message.
                        Checkout https://github.com/knuddelsgmbh/jtokkit for more details.
                                """,
                defaultValue = "100")
        private int chunk_overlap;

        @ConfigProperty(
                description =
                        """
                        RecursiveCharacterTextSplitter splitter option. Options are: r50k_base, p50k_base, p50k_edit and cl100k_base.
                        Checkout https://github.com/knuddelsgmbh/jtokkit for more details.
                                """,
                defaultValue = "cl100k_base")
        private String length_function;
    }

    @AgentConfig(
            name = "Text normaliser",
            description = """
            Apply normalisation to the text.
            """)
    @Data
    public static class TextNormaliserConfig {
        @ConfigProperty(
                description =
                        """
                        Whether to make the text lowercase.
                                """,
                defaultValue = "true")
        @JsonProperty("make-lowercase")
        private boolean makeLowercase;

        @ConfigProperty(
                description =
                        """
                        Whether to trim spaces from the text.
                                """,
                defaultValue = "true")
        @JsonProperty("trim-spaces")
        private boolean trimSpaces;
    }

    @AgentConfig(
            name = "Document to JSON",
            description =
                    """
            Convert raw text document to JSON. The result will be a JSON object with the text content in the specified field.
            """)
    @Data
    public static class DocumentToJsonConfig {
        @ConfigProperty(
                description =
                        """
                        Field name to write the text content to.
                                """,
                defaultValue = "text")
        @JsonProperty("text-field")
        private String textField;

        @ConfigProperty(
                description =
                        """
                        Whether to copy the message properties/headers in the output message.
                                """,
                defaultValue = "true")
        @JsonProperty("copy-properties")
        private boolean copyProperties;
    }
}
