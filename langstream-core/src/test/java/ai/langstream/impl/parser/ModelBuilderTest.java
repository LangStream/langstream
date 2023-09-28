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

import ai.langstream.api.model.Application;
import ai.langstream.api.model.Gateway;
import ai.langstream.api.model.Secrets;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@Slf4j
class ModelBuilderTest {

    static class StrChecksumFunction implements ModelBuilder.ChecksumFunction {
        final StringBuilder builder = new StringBuilder();

        @Override
        public void appendFile(String filename, byte[] data) {
            builder.append(filename)
                    .append(":")
                    .append(new String(data, StandardCharsets.UTF_8))
                    .append(",");
        }

        @Override
        public String digest() {
            return builder.toString();
        }
    }

    @Test
    void testPyChecksum() throws Exception {
        final Path path = Files.createTempDirectory("langstream");
        final Path python = Files.createDirectories(path.resolve("python"));
        Files.writeString(python.resolve("script.py"), "print('hello world')");
        Files.writeString(python.resolve("script2.py"), "print('hello world2')");
        final Path pythonSubdir = Files.createDirectories(python.resolve("asubdir"));
        Files.writeString(pythonSubdir.resolve("script2.py"), "print('hello world3')");
        ModelBuilder.ApplicationWithPackageInfo applicationWithPackageInfo =
                ModelBuilder.buildApplicationInstance(
                        List.of(path),
                        null,
                        null,
                        new StrChecksumFunction(),
                        new StrChecksumFunction());
        Assertions.assertEquals(
                "asubdir/script2.py:print('hello world3'),script.py:print('hello world'),script2.py:print('hello world2'),",
                applicationWithPackageInfo.getPyBinariesDigest());
        applicationWithPackageInfo =
                ModelBuilder.buildApplicationInstance(List.of(path), null, null);
        Assertions.assertEquals(
                "f4b3d77c3886ece4247c9547f46491dedfa0650dde553cbbc4df05601688e329",
                applicationWithPackageInfo.getPyBinariesDigest());
        Assertions.assertNull(applicationWithPackageInfo.getJavaBinariesDigest());
    }

    @Test
    void testJavaLibChecksum() throws Exception {
        final Path path = Files.createTempDirectory("langstream");
        final Path java = Files.createDirectories(path.resolve("java"));
        final Path lib = Files.createDirectories(java.resolve("lib"));
        Files.writeString(lib.resolve("my-jar-1.jar"), "some bin content");
        Files.writeString(lib.resolve("my-jar-2.jar"), "some bin content2");
        ModelBuilder.ApplicationWithPackageInfo applicationWithPackageInfo =
                ModelBuilder.buildApplicationInstance(
                        List.of(path),
                        null,
                        null,
                        new StrChecksumFunction(),
                        new StrChecksumFunction());
        Assertions.assertEquals(
                "my-jar-1.jar:some bin content,my-jar-2.jar:some bin content2,",
                applicationWithPackageInfo.getJavaBinariesDigest());
        applicationWithPackageInfo =
                ModelBuilder.buildApplicationInstance(List.of(path), null, null);
        Assertions.assertEquals(
                "589c0438a29fe804da9f1848b8c6ecb291a55f1e665b0f92a4d46929c09e117c",
                applicationWithPackageInfo.getJavaBinariesDigest());
        Assertions.assertNull(applicationWithPackageInfo.getPyBinariesDigest());
    }

    @Test
    void testParseGateway() throws Exception {
        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
                                        "module.yaml",
                                        """
                                module: "module-1"
                                id: "pipeline-1"
                                pipeline:
                                  - name: "step1"
                                    type: "noop"

                                """,
                                        "gateways.yaml",
                                        """
                                gateways:
                                - id: gw
                                  type: produce
                                  topic: t1
                                  authentication:
                                    provider: google
                                    configuration: {}
                                """),
                                null,
                                null)
                        .getApplication();
        final List<Gateway> gateways = applicationInstance.getGateways().gateways();
        Assertions.assertEquals(1, gateways.size());
        final Gateway gateway = gateways.get(0);
        assertEquals("gw", gateway.getId());
        assertEquals("t1", gateway.getTopic());
        assertEquals("google", gateway.getAuthentication().getProvider());
        assertTrue(gateway.getAuthentication().isAllowTestMode());
    }

    @Test
    void testParseRealDirectoryWithDefaults() throws Exception {
        final Path path = Paths.get("src/test/resources/application1");
        String instanceContent =
                """
                instance:
                   globals:
                      var1: "value1"
                      var2: "value2"
                      var-array1:
                        - "value1"
                        - "value2"
                      var-array2:
                        - "value1"
                        - "value2"
                """;
        String secretsContent =
                """
                secrets:
                  - id: secret1
                    data:
                      var2: secret-value2
                      var1: secret-value1
                """;

        // with an instance file with globals
        ModelBuilder.ApplicationWithPackageInfo applicationWithPackageInfo =
                ModelBuilder.buildApplicationInstance(
                        List.of(path),
                        instanceContent,
                        secretsContent,
                        new StrChecksumFunction(),
                        new StrChecksumFunction());
        Application applicationInstance = applicationWithPackageInfo.getApplication();
        Map<String, Object> globals = applicationInstance.getInstance().globals();
        assertEquals("value1", globals.get("var1"));
        assertEquals("value2", globals.get("var2"));
        assertEquals("default-value3", globals.get("var3"));

        assertEquals(List.of("value1", "value2"), globals.get("var-array1"));
        assertEquals(List.of("value1", "value2"), globals.get("var-array2"));
        assertEquals(
                List.of("default-value1", "default-value2", "default-value3"),
                globals.get("var-array3"));

        Secrets secrets = applicationInstance.getSecrets();

        Map<String, Object> secret1 = secrets.secrets().get("secret1").data();
        assertEquals("secret-value1", secret1.get("var1"));
        assertEquals("secret-value2", secret1.get("var2"));

        // with an instance file without globals
        String instanceContentWithoutGlobals =
                """
                instance:
                   computeCluster:
                      type: kafka
                """;
        applicationWithPackageInfo =
                ModelBuilder.buildApplicationInstance(
                        List.of(path),
                        instanceContentWithoutGlobals,
                        secretsContent,
                        new StrChecksumFunction(),
                        new StrChecksumFunction());
        applicationInstance = applicationWithPackageInfo.getApplication();
        globals = applicationInstance.getInstance().globals();
        assertEquals("default-value1", globals.get("var1"));
        assertEquals("default-value2", globals.get("var2"));
        assertEquals("default-value3", globals.get("var3"));

        // with an instance file with empty globals
        String instanceContentWithEmptyGlobals =
                """
                instance:
                    globals:
                """;
        applicationWithPackageInfo =
                ModelBuilder.buildApplicationInstance(
                        List.of(path),
                        instanceContentWithEmptyGlobals,
                        secretsContent,
                        new StrChecksumFunction(),
                        new StrChecksumFunction());
        applicationInstance = applicationWithPackageInfo.getApplication();
        globals = applicationInstance.getInstance().globals();
        assertEquals("default-value1", globals.get("var1"));
        assertEquals("default-value2", globals.get("var2"));
        assertEquals("default-value3", globals.get("var3"));

        // without an instance file
        applicationWithPackageInfo =
                ModelBuilder.buildApplicationInstance(
                        List.of(path),
                        null,
                        secretsContent,
                        new StrChecksumFunction(),
                        new StrChecksumFunction());
        applicationInstance = applicationWithPackageInfo.getApplication();
        globals = applicationInstance.getInstance().globals();
        assertEquals("default-value1", globals.get("var1"));
        assertEquals("default-value2", globals.get("var2"));
        assertEquals("default-value3", globals.get("var3"));
    }
}
