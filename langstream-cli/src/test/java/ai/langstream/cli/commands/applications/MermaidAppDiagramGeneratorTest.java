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
package ai.langstream.cli.commands.applications;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class MermaidAppDiagramGeneratorTest {

    @Test
    void generate() throws Exception {
        final String jsonValue =
                new String(
                        MermaidAppDiagramGeneratorTest.class
                                .getClassLoader()
                                .getResourceAsStream("expected-get.json")
                                .readAllBytes(),
                        StandardCharsets.UTF_8);

        final String result = MermaidAppDiagramGenerator.generate(jsonValue);
        assertEquals(
                "flowchart LR\n"
                        + "\n"
                        + "external-client((Client))\n"
                        + "\n"
                        + "external-sink-agent-write-to-astra-sink-1([\"External system\"])\n"
                        + "\n"
                        + "external-source-agent-extract-text-s3-source-1([\"External system\"])\n"
                        + "\n"
                        + "subgraph resources[\"Resources\"]\n"
                        + "resource-openai___azure___configuration(\"OpenAI Azure configuration\")\n"
                        + "end\n"
                        + "\n"
                        + "subgraph streaming-cluster[\"Topics\"]\n"
                        + "topic-chunks-topic([\"chunks-topic\"])\n"
                        + "end\n"
                        + "\n"
                        + "subgraph gateways[\"Gateways\"]\n"
                        + "gateway-consume-chunks[/\"consume-chunks\"\\]\n"
                        + "end\n"
                        + "\n"
                        + "subgraph pipeline-extract-text[\"Pipeline: <b>extract-text</b>\"]\n"
                        + "agent-extract-text-s3-source-1(\"Read from S3\")\n"
                        + "agent-extract-text-text-extractor-2(\"Extract text\")\n"
                        + "agent-extract-text-text-normaliser-3(\"Normalise text\")\n"
                        + "agent-extract-text-language-detector-4(\"Detect language\")\n"
                        + "agent-extract-text-text-splitter-5(\"Split into chunks\")\n"
                        + "agent-extract-text-document-to-json-6(\"Convert to structured data\")\n"
                        + "agent-extract-text-compute-7(\"prepare-structure\")\n"
                        + "agent-step1(\"compute-embeddings\")\n"
                        + "end\n"
                        + "\n"
                        + "subgraph pipeline-write-to-astra[\"Pipeline: <b>write-to-astra</b>\"]\n"
                        + "agent-write-to-astra-sink-1(\"Write to AstraDB\")\n"
                        + "end\n"
                        + "\n"
                        + "agent-extract-text-s3-source-1-.->external-source-agent-extract-text-s3-source-1\n"
                        + "linkStyle 0 stroke:#82E0AA\n"
                        + "agent-write-to-astra-sink-1-.->topic-chunks-topic\n"
                        + "linkStyle 1 stroke:#82E0AA\n"
                        + "gateway-consume-chunks-.->topic-chunks-topic\n"
                        + "external-client-->gateways\n"
                        + "agent-extract-text-s3-source-1-->agent-extract-text-text-extractor-2\n"
                        + "linkStyle 4 stroke:#5DADE2\n"
                        + "agent-extract-text-text-extractor-2-->agent-extract-text-text-normaliser-3\n"
                        + "linkStyle 5 stroke:#5DADE2\n"
                        + "agent-extract-text-text-normaliser-3-->agent-extract-text-language-detector-4\n"
                        + "linkStyle 6 stroke:#5DADE2\n"
                        + "agent-extract-text-language-detector-4-->agent-extract-text-text-splitter-5\n"
                        + "linkStyle 7 stroke:#5DADE2\n"
                        + "agent-extract-text-text-splitter-5-->agent-extract-text-document-to-json-6\n"
                        + "linkStyle 8 stroke:#5DADE2\n"
                        + "agent-extract-text-document-to-json-6-->agent-extract-text-compute-7\n"
                        + "linkStyle 9 stroke:#5DADE2\n"
                        + "agent-extract-text-compute-7-->agent-step1\n"
                        + "linkStyle 10 stroke:#5DADE2\n"
                        + "agent-step1-->topic-chunks-topic\n"
                        + "linkStyle 11 stroke:#F4D03F\n"
                        + "agent-step1-.->resource-openai___azure___configuration\n"
                        + "linkStyle 12 stroke:#5DADE2\n"
                        + "agent-write-to-astra-sink-1-->external-sink-agent-write-to-astra-sink-1\n"
                        + "linkStyle 13 stroke:#F4D03F\n",
                result);
    }

    @Test
    void generateNoData() throws Exception {
        assertNotNull(MermaidAppDiagramGenerator.generate("{\"application\": {}}"));
        assertNotNull(MermaidAppDiagramGenerator.generate("{\"application\": {\"gateways\": {}}}"));
    }
}
