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
import ai.langstream.api.codestorage.LocalZipFileArchiveFile;
import ai.langstream.api.codestorage.UploadableCodeArchive;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LocalDiskCodeStorage implements CodeStorage {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path rootPath;

    private final Path metadataFilePath;

    private final CopyOnWriteArrayList<CodeArchiveMetadata> archives = new CopyOnWriteArrayList<>();

    public LocalDiskCodeStorage(Path rootPath) {
        this.rootPath = rootPath;
        this.metadataFilePath = rootPath.resolve("metadata.json");

        initMetadata();
    }

    @SneakyThrows
    private void initMetadata() {
        File file = metadataFilePath.toFile();
        log.info("Loading archives metadata from {}", file);
        if (file.exists()) {
            // load the metadata
            List<CodeArchiveMetadata> codeArchives =
                    MAPPER.readValue(file, new TypeReference<>() {});
            archives.addAll(codeArchives);
        } else {
            log.info("No metadata file found at {}, creating an empty file", file);
            persistMetadata();
        }
    }

    @SneakyThrows
    private void persistMetadata() {
        if (!Files.isDirectory(rootPath)) {
            Files.createDirectories(rootPath);
        }
        File file = metadataFilePath.toFile();
        log.info("Loading {} archives metadata from {}", archives.size(), file);
        MAPPER.writeValue(file, new ArrayList<>(archives));
    }

    @Override
    public CodeArchiveMetadata storeApplicationCode(
            String tenant, String applicationId, String version, UploadableCodeArchive codeArchive)
            throws CodeStorageException {

        try {
            String uniqueId =
                    tenant + "_" + applicationId + "_" + version + "_" + UUID.randomUUID();
            log.info(
                    "Storing code archive {} for tenant {} and application {} with version {}",
                    uniqueId,
                    tenant,
                    applicationId,
                    version);
            Path resolve = rootPath.resolve(uniqueId);
            Files.copy(codeArchive.getData(), resolve);
            CodeArchiveMetadata archiveMetadata =
                    new CodeArchiveMetadata(
                            tenant,
                            uniqueId,
                            applicationId,
                            codeArchive.getPyBinariesDigest(),
                            codeArchive.getJavaBinariesDigest());
            archives.add(archiveMetadata);
            persistMetadata();
            return archiveMetadata;
        } catch (IOException err) {
            throw new CodeStorageException(err);
        }
    }

    @Override
    public void downloadApplicationCode(
            String tenant, String codeStoreId, DownloadedCodeHandled codeArchive)
            throws CodeStorageException {
        CodeArchiveMetadata metadata = describeApplicationCode(tenant, codeStoreId);
        Path resolve = rootPath.resolve(metadata.codeStoreId());
        codeArchive.accept(new LocalZipFileArchiveFile(resolve));
    }

    @Override
    public CodeArchiveMetadata describeApplicationCode(String tenant, String codeStoreId)
            throws CodeStorageException {
        return archives.stream()
                .filter(c -> c.tenant().equals(tenant) && c.codeStoreId().equals(codeStoreId))
                .findFirst()
                .orElseThrow(
                        (() ->
                                new CodeStorageException(
                                        "No code archive found for tenant "
                                                + tenant
                                                + " and codeStoreId "
                                                + codeStoreId)));
    }

    @Override
    public void deleteApplicationCode(String tenant, String codeStoreId) {
        archives.stream()
                .filter(c -> c.tenant().equals(tenant) && c.codeStoreId().equals(codeStoreId))
                .findFirst()
                .ifPresent(
                        c -> {
                            log.info("Deleting archive {}", c);
                            archives.remove(c);
                            persistMetadata();
                        });
    }

    @Override
    public void deleteApplication(String tenant, String application) {
        archives.stream()
                .filter(c -> c.tenant().equals(tenant) && c.applicationId().equals(application))
                .findFirst()
                .ifPresent(
                        c -> {
                            log.info("Deleting archive {}", c);
                            archives.remove(c);
                            persistMetadata();
                        });
    }

    @Override
    public void close() {}
}
