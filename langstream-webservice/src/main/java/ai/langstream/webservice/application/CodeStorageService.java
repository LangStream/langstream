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
package ai.langstream.webservice.application;

import ai.langstream.api.codestorage.CodeArchiveMetadata;
import ai.langstream.api.codestorage.CodeStorage;
import ai.langstream.api.codestorage.CodeStorageException;
import ai.langstream.api.codestorage.CodeStorageRegistry;
import ai.langstream.api.codestorage.DownloadedCodeArchive;
import ai.langstream.impl.codestorage.LocalFileUploadableCodeArchive;
import ai.langstream.webservice.config.StorageProperties;
import java.nio.file.Path;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import lombok.SneakyThrows;
import lombok.extern.jbosslog.JBossLog;
import org.springframework.stereotype.Service;

@Service
@JBossLog
public class CodeStorageService {

    private final CodeStorage codeStorage;

    public CodeStorageService(StorageProperties storageProperties) {
        log.info("Loading CodeStorage implementation for " + storageProperties);
        codeStorage =
                CodeStorageRegistry.getCodeStorage(storageProperties.getCode().getType(),
                        storageProperties.getCode().getConfiguration());
    }

    public String deployApplicationCodeStorage(String tenant, String application, Path zipFile) throws CodeStorageException {
       String uuid = UUID.randomUUID().toString();
        CodeArchiveMetadata archiveMetadata = codeStorage.storeApplicationCode(tenant, application, uuid,
                new LocalFileUploadableCodeArchive(zipFile));
        if (!Objects.equals(tenant, archiveMetadata.tenant())
            || !Objects.equals(application, archiveMetadata.applicationId())) {
            throw new CodeStorageException("Invalid archive metadata " + archiveMetadata +
                    " for tenant " + tenant + " and application " + application);
        }
        return archiveMetadata.codeStoreId();
    }

    public byte[] downloadApplicationCode(String tenant, String applicationId, String codeStoreId) throws CodeStorageException {
        final CodeArchiveMetadata codeArchiveMetadata = codeStorage.describeApplicationCode(tenant, codeStoreId);
        if (codeArchiveMetadata == null) {
            throw new IllegalArgumentException("Invalid code archive " + codeStoreId + " for tenant " + tenant + " and application " + applicationId);
        }
        if (!Objects.equals(codeArchiveMetadata.tenant(), tenant)) {
            throw new IllegalArgumentException("Invalid tenant " + tenant + " for code archive " + codeStoreId);
        }
        if (!Objects.equals(codeArchiveMetadata.applicationId(), applicationId)) {
            throw new IllegalArgumentException("Invalid application id " + tenant + " for code archive " + codeStoreId);
        }
        AtomicReference<byte[]> result = new AtomicReference<>();
        codeStorage.downloadApplicationCode(tenant, codeStoreId, new CodeStorage.DownloadedCodeHandled() {
            @Override
            @SneakyThrows
            public void accept(DownloadedCodeArchive archive) {
                result.set(archive.getData());
            }
        });
        return result.get();
    }

}
