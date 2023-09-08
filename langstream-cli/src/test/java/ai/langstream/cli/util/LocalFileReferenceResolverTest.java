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
package ai.langstream.cli.util;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LocalFileReferenceResolverTest {

    @Test
    void resolveFileReferencesInSecrets() throws Exception {
        String content =
                "secrets:\n"
                        + "  - name: vertex-ai\n"
                        + "    id: vertex-ai\n"
                        + "    data:\n"
                        + "      url: https://us-central1-aiplatform.googleapis.com\n"
                        + "      token: xxx\n"
                        + "      serviceAccountJson: \"<file:service.account.json>\"\n"
                        + "      region: us-central1\n"
                        + "      project: myproject\n"
                        + "      list:\n"
                        + "        - \"<file:some-text-file.txt>\"\n"
                        + "        - \"<file:some-text-file.txt>\"\n"
                        + "  - name: astra\n"
                        + "    id: astra\n"
                        + "    data:\n"
                        + "      username: xxx\n"
                        + "      password: xxx\n"
                        + "      secureBundle: \"<file:secure-connect-bundle.zip>\"\n";

        String result =
                LocalFileReferenceResolver.resolveFileReferencesInYAMLFile(
                        content,
                        filename -> {
                            switch (filename) {
                                case "service.account.json":
                                    return " { \"user-id\": \"xxx\", \"password\": \"xxx\" }\n";
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
                "---\n"
                        + "secrets:\n"
                        + "- name: \"vertex-ai\"\n"
                        + "  id: \"vertex-ai\"\n"
                        + "  data:\n"
                        + "    url: \"https://us-central1-aiplatform.googleapis.com\"\n"
                        + "    token: \"xxx\"\n"
                        + "    serviceAccountJson: \" { \\\"user-id\\\": \\\"xxx\\\", \\\"password\\\": \\\"xxx\\\" }\\n\"\n"
                        + "    region: \"us-central1\"\n"
                        + "    project: \"myproject\"\n"
                        + "    list:\n"
                        + "    - \"text content with \\\" and ' and \\n and \\r and \\t\"\n"
                        + "    - \"text content with \\\" and ' and \\n and \\r and \\t\"\n"
                        + "- name: \"astra\"\n"
                        + "  id: \"astra\"\n"
                        + "  data:\n"
                        + "    username: \"xxx\"\n"
                        + "    password: \"xxx\"\n"
                        + "    secureBundle: \"base64:zip-content\"\n");
    }

    @Test
    void dontRewriteFileWithoutReferences() throws Exception {
        String content =
                "secrets:\n"
                        + "  - name: vertex-ai\n"
                        + "    id: vertex-ai\n"
                        + "    data:\n"
                        + "      url: https://us-central1-aiplatform.googleapis.com\n"
                        + "  - name: astra\n"
                        + "    id: astra\n"
                        + "    data:\n"
                        + "      username: xxx\n"
                        + "      password: xxx\n";

        String result =
                LocalFileReferenceResolver.resolveFileReferencesInYAMLFile(
                        content,
                        filename -> {
                            throw new IllegalStateException();
                        });

        assertEquals(result, content);
    }

    @Test
    void testResolveBinaryAndTextFiles(@TempDir Path tempDir) throws Exception {
        String content =
                "secrets:\n"
                        + "  - \"<file:some-text-file.txt>\"\n"
                        + "  - \"<file:some-binary-file.bin>\"\n";

        Path file = tempDir.resolve("instance.yaml");
        Files.write(file, content.getBytes(StandardCharsets.UTF_8));
        Files.write(
                tempDir.resolve("some-text-file.txt"),
                "some text".getBytes(StandardCharsets.UTF_8));
        Files.write(tempDir.resolve("some-binary-file.bin"), new byte[] {0x01, 0x02, 0x03});

        String result = LocalFileReferenceResolver.resolveFileReferencesInYAMLFile(file);

        assertEquals(result, "---\n" + "secrets:\n" + "- \"some text\"\n" + "- \"base64:AQID\"\n");
    }

    @Test
    void testReplaceEnvVariables() throws Exception {
        String JAVA_HOME = System.getenv("JAVA_HOME");
        assertNotNull(
                JAVA_HOME, "This test assumes that you have a JAVA_HOME environment variable");

        String content =
                "secrets:\n"
                        + "  - name: some-secret\n"
                        + "    id: some-id\n"
                        + "    data:\n"
                        + "      variable-1-from-env: ${JAVA_HOME:-defaultValue}\n"
                        + "      variable-2-from-env: ${USE_DEFAULT:-defaultValue}\n"
                        + "      variable-3-from-env: ${java_home:-defaultValue}\n";

        System.getenv().forEach((k, v) -> System.out.println(k + " = " + v));

        String result =
                LocalFileReferenceResolver.resolveFileReferencesInYAMLFile(content, (___) -> "");
        assertEquals(
                result,
                "---\n"
                        + "secrets:\n"
                        + "- name: \"some-secret\"\n"
                        + "  id: \"some-id\"\n"
                        + "  data:\n"
                        + "    variable-1-from-env: \""
                        + JAVA_HOME
                        + "\"\n"
                        + "    variable-2-from-env: \"defaultValue\"\n"
                        + "    variable-3-from-env: \"defaultValue\"\n");
    }
}
