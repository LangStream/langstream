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
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
}
