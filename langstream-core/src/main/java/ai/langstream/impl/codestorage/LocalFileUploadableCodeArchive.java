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
package ai.langstream.impl.codestorage;

import ai.langstream.api.codestorage.UploadableCodeArchive;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class LocalFileUploadableCodeArchive implements UploadableCodeArchive {
    private final Path zipFile;
    private final String pyBinariesDigest;
    private final String javaBinariesDigest;

    @Override
    public InputStream getData() throws IOException {
        return Files.newInputStream(zipFile);
    }

    @Override
    public String getPyBinariesDigest() {
        return pyBinariesDigest;
    }

    @Override
    public String getJavaBinariesDigest() {
        return javaBinariesDigest;
    }
}
