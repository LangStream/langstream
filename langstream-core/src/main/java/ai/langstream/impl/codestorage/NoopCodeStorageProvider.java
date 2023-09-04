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

import ai.langstream.api.codestorage.CodeArchiveMetadata;
import ai.langstream.api.codestorage.CodeStorage;
import ai.langstream.api.codestorage.CodeStorageException;
import ai.langstream.api.codestorage.CodeStorageProvider;
import ai.langstream.api.codestorage.DownloadedCodeArchive;
import ai.langstream.api.codestorage.UploadableCodeArchive;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NoopCodeStorageProvider implements CodeStorageProvider {

    @Override
    public CodeStorage createImplementation(
            String codeStorageType, Map<String, Object> configuration) {
        return new CodeStorage() {
            private final Map<String, CodeArchiveMetadata> archives = new ConcurrentHashMap<>();

            @Override
            public CodeArchiveMetadata storeApplicationCode(
                    String tenant,
                    String applicationId,
                    String version,
                    UploadableCodeArchive codeArchive) {
                final String codeArchiveId = computeCodeArchiveId(tenant, applicationId);
                CodeArchiveMetadata archiveMetadata =
                        new CodeArchiveMetadata(
                                tenant,
                                codeArchiveId,
                                applicationId,
                                codeArchive.getPyBinariesDigest(),
                                codeArchive.getJavaBinariesDigest());
                archives.put(archiveMetadata.codeStoreId(), archiveMetadata);
                return archiveMetadata;
            }

            @Override
            public void downloadApplicationCode(
                    String tenant, String codeStoreId, DownloadedCodeHandled codeArchive)
                    throws CodeStorageException {
                codeArchive.accept(
                        new DownloadedCodeArchive() {

                            @Override
                            public byte[] getData() {
                                return "content-of-the-code-archive-%s-%s"
                                        .formatted(tenant, codeStoreId)
                                        .getBytes(StandardCharsets.UTF_8);
                            }

                            @Override
                            public InputStream getInputStream() {
                                throw new UnsupportedOperationException();
                            }

                            @Override
                            public void extractTo(Path directory) {
                                log.info(
                                        "CodeArchive should have been extracted to {}, but this is a no-op implementation",
                                        directory);
                            }
                        });
            }

            @Override
            public CodeArchiveMetadata describeApplicationCode(String tenant, String codeStoreId) {
                return archives.get(codeStoreId);
            }

            @Override
            public void deleteApplicationCode(String tenant, String codeStoreId) {
                archives.remove(codeStoreId);
            }

            @Override
            public void deleteApplication(String tenant, String application) {}

            @Override
            public void close() {}
        };
    }

    public static String computeCodeArchiveId(String tenant, String applicationId) {
        return "%s-%s".formatted(tenant, applicationId);
    }

    @Override
    public boolean supports(String codeStorageType) {
        return "none".equals(codeStorageType);
    }
}
