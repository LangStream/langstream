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
package ai.langstream.impl.parser;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ModelBuilderTest {

    @Test
    void resolveFileReferencesInSecrets() throws Exception {
        String content =
                """
                secrets:
                  - name: vertex-ai
                    id: vertex-ai
                    data:
                      url: https://us-central1-aiplatform.googleapis.com
                      token: xxx
                      serviceAccountJson: "<file:service.account.json>"
                      region: us-central1
                      project: myproject
                      list:
                        - "<file:some-text-file.txt>"
                        - "<file:some-text-file.txt>"
                  - name: astra
                    id: astra
                    data:
                      username: xxx
                      password: xxx
                      secureBundle: "<file:secure-connect-bundle.zip>"
                """;

        String result =
                ModelBuilder.resolveFileReferencesInYAMLFile(
                        content,
                        filename -> {
                            switch (filename) {
                                case "service.account.json":
                                    return """
                             { "user-id": "xxx", "password": "xxx" }
                            """;
                                case "secure-connect-bundle.zip":
                                    return "base64:zip-content";
                                case "some-text-file.txt":
                                    return "text content with \" and ' and \n and \r and \t";
                                default:
                                    throw new IllegalStateException();
                            }
                        });

        assertEquals(
                result,
                """
                  ---
                  secrets:
                  - name: "vertex-ai"
                    id: "vertex-ai"
                    data:
                      url: "https://us-central1-aiplatform.googleapis.com"
                      token: "xxx"
                      serviceAccountJson: " { \\"user-id\\": \\"xxx\\", \\"password\\": \\"xxx\\" }\\n"
                      region: "us-central1"
                      project: "myproject"
                      list:
                      - "text content with \\" and ' and \\n and \\r and \\t"
                      - "text content with \\" and ' and \\n and \\r and \\t"
                  - name: "astra"
                    id: "astra"
                    data:
                      username: "xxx"
                      password: "xxx"
                      secureBundle: "base64:zip-content"
                  """);
    }

    @Test
    void dontRewriteFileWithoutReferences() throws Exception {
        String content =
                """
                secrets:
                  - name: vertex-ai
                    id: vertex-ai
                    data:
                      url: https://us-central1-aiplatform.googleapis.com
                  - name: astra
                    id: astra
                    data:
                      username: xxx
                      password: xxx
                """;

        String result =
                ModelBuilder.resolveFileReferencesInYAMLFile(
                        content,
                        filename -> {
                            throw new IllegalStateException();
                        });

        assertEquals(result, content);
    }

    @Test
    void testResolveBinaryAndTextFiles(@TempDir Path tempDir) throws Exception {
        String content =
                """
                secrets:
                  - "<file:some-text-file.txt>"
                  - "<file:some-binary-file.bin>"
                """;

        Path file = tempDir.resolve("instance.yaml");
        Files.write(file, content.getBytes(StandardCharsets.UTF_8));
        Files.write(
                tempDir.resolve("some-text-file.txt"),
                "some text".getBytes(StandardCharsets.UTF_8));
        Files.write(tempDir.resolve("some-binary-file.bin"), new byte[] {0x01, 0x02, 0x03});

        String result = ModelBuilder.resolveFileReferencesInYAMLFile(file);

        assertEquals(
                result,
                """
                      ---
                      secrets:
                      - "some text"
                      - "base64:AQID"
                      """);
    }
}
