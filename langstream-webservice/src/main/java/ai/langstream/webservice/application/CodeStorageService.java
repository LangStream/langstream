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
import ai.langstream.api.webservice.application.ApplicationCodeInfo;
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
                CodeStorageRegistry.getCodeStorage(
                        storageProperties.getCode().getType(),
                        storageProperties.getCode().getConfiguration());
    }

    public String deployApplicationCodeStorage(
            String tenant,
            String applicationId,
            Path zipFile,
            String pyBinariesDigest,
            String javaBinariesDigest)
            throws CodeStorageException {
        String uuid = UUID.randomUUID().toString();
        CodeArchiveMetadata archiveMetadata =
                codeStorage.storeApplicationCode(
                        tenant,
                        applicationId,
                        uuid,
                        new LocalFileUploadableCodeArchive(
                                zipFile, pyBinariesDigest, javaBinariesDigest));
        if (!Objects.equals(tenant, archiveMetadata.tenant())
                || !Objects.equals(applicationId, archiveMetadata.applicationId())) {
            throw new CodeStorageException(
                    "Invalid archive metadata "
                            + archiveMetadata
                            + " for tenant "
                            + tenant
                            + " and application "
                            + applicationId);
        }
        return archiveMetadata.codeStoreId();
    }

    public byte[] downloadApplicationCode(String tenant, String applicationId, String codeArchiveId)
            throws CodeStorageException {
        final CodeArchiveMetadata codeArchiveMetadata =
                codeStorage.describeApplicationCode(tenant, codeArchiveId);
        checkCodeArchiveMetadata(tenant, applicationId, codeArchiveId, codeArchiveMetadata);
        AtomicReference<byte[]> result = new AtomicReference<>();
        codeStorage.downloadApplicationCode(
                tenant,
                codeArchiveId,
                new CodeStorage.DownloadedCodeHandled() {
                    @Override
                    @SneakyThrows
                    public void accept(DownloadedCodeArchive archive) {
                        result.set(archive.getData());
                    }
                });
        return result.get();
    }

    private void checkCodeArchiveMetadata(
            String tenant,
            String applicationId,
            String codeArchiveId,
            CodeArchiveMetadata codeArchiveMetadata) {
        if (codeArchiveMetadata == null) {
            throw new IllegalArgumentException(
                    "Invalid code archive "
                            + codeArchiveId
                            + " for tenant "
                            + tenant
                            + " and application "
                            + applicationId);
        }
        if (!Objects.equals(codeArchiveMetadata.tenant(), tenant)) {
            throw new IllegalArgumentException(
                    "Invalid tenant " + tenant + " for code archive " + codeArchiveId);
        }
        if (!Objects.equals(codeArchiveMetadata.applicationId(), applicationId)) {
            throw new IllegalArgumentException(
                    "Invalid application id " + tenant + " for code archive " + codeArchiveId);
        }
    }

    public ApplicationCodeInfo getApplicationCodeInfo(
            String tenant, String applicationId, String codeArchiveId) throws CodeStorageException {
        final CodeArchiveMetadata codeArchiveMetadata =
                codeStorage.describeApplicationCode(tenant, codeArchiveId);
        checkCodeArchiveMetadata(tenant, applicationId, codeArchiveId, codeArchiveMetadata);

        final ApplicationCodeInfo.Digests digests = new ApplicationCodeInfo.Digests();
        if (codeArchiveMetadata.javaBinariesDigest() != null) {
            digests.setJava(codeArchiveMetadata.javaBinariesDigest());
        }
        if (codeArchiveMetadata.pyBinariesDigest() != null) {
            digests.setPython(codeArchiveMetadata.pyBinariesDigest());
        }
        return new ApplicationCodeInfo(digests);
    }
}
