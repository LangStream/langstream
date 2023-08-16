/**
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
package com.datastax.oss.sga.impl.codestorage;

import com.datastax.oss.sga.api.codestorage.CodeArchiveMetadata;
import com.datastax.oss.sga.api.codestorage.CodeStorage;
import com.datastax.oss.sga.api.codestorage.CodeStorageException;
import com.datastax.oss.sga.api.codestorage.CodeStorageProvider;
import com.datastax.oss.sga.api.codestorage.DownloadedCodeArchive;
import com.datastax.oss.sga.api.codestorage.UploadableCodeArchive;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

@Slf4j
public class NoopCodeStorageProvider implements CodeStorageProvider {

    @Override
    public CodeStorage createImplementation(String codeStorageType, Map<String, Object> configuration) {
        return new CodeStorage() {
            private Map<String, CodeArchiveMetadata> archives = new ConcurrentHashMap<>();
            @Override
            public CodeArchiveMetadata storeApplicationCode(String tenant, String applicationId, String version, UploadableCodeArchive codeArchive) throws CodeStorageException {
                final String code = "%s-%s".formatted(tenant, applicationId);
                CodeArchiveMetadata archiveMetadata = new CodeArchiveMetadata(tenant, code, applicationId);
                archives.put(archiveMetadata.codeStoreId(), archiveMetadata);
                return archiveMetadata;
            }

            @Override
            public void downloadApplicationCode(String tenant, String codeStoreId, DownloadedCodeHandled codeArchive) throws CodeStorageException {
                codeArchive.accept(new DownloadedCodeArchive() {
                    @Override
                    public void extractTo(Path directory) throws CodeStorageException {
                        log.info("CodeArchive should have been extracted to {}, but this is a no-op implementation", directory);
                    }
                });
            }

            @Override
            public CodeArchiveMetadata describeApplicationCode(String tenant, String codeStoreId) throws CodeStorageException {
                return archives.get(codeStoreId);
            }

            @Override
            public void deleteApplicationCode(String tenant, String codeStoreId) throws CodeStorageException {
                archives.remove(codeStoreId);
            }

            @Override
            public void deleteApplication(String tenant, String application) throws CodeStorageException {

            }

            @Override
            public void close() {
            }
        };
    }

    @Override
    public boolean supports(String codeStorageType) {
        return "none".equals(codeStorageType);
    }
}
