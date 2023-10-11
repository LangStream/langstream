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
                "flowchart TB\n"
                        + "\n"
                        + "subgraph resources[\"<b>Resources</b>\"]\n"
                        + "OpenAIAzureconfiguration[\"Service: OpenAI Azure configuration\"]\n"
                        + "end\n"
                        + "\n"
                        + "subgraph streaming-cluster[\"<b>Streaming</b>\"]\n"
                        + "chunks-topic\n"
                        + "chunks-topic --> write-to-astra-sink-1\n"
                        + "end\n"
                        + "\n"
                        + "subgraph gateways[\"<b>Gateways</b>\"]\n"
                        + "consume-chunks --> chunks-topic\n"
                        + "end\n"
                        + "\n"
                        + "subgraph extract-text[\"<b>extract-text</b>\"]\n"
                        + "extract-text-s3-source-1(\"Read from S3\") --> extract-text-text-extractor-2\n"
                        + "extract-text-text-extractor-2(\"Extract text\") --> extract-text-text-normaliser-3\n"
                        + "extract-text-text-normaliser-3(\"Normalise text\") --> extract-text-language-detector-4\n"
                        + "extract-text-language-detector-4(\"Detect language\") --> extract-text-text-splitter-5\n"
                        + "extract-text-text-splitter-5(\"Split into chunks\") --> extract-text-document-to-json-6\n"
                        + "extract-text-document-to-json-6(\"Convert to structured data\") --> extract-text-compute-7\n"
                        + "extract-text-compute-7(\"prepare-structure\") --> step1\n"
                        + "step1(\"compute-embeddings\") --> chunks-topic\n"
                        + "step1(\"compute-embeddings\") --> OpenAIAzureconfiguration\n"
                        + "end\n"
                        + "\n"
                        + "subgraph write-to-astra[\"<b>write-to-astra</b>\"]\n"
                        + "end\n"
                        + "\n",
                result);
    }
}
