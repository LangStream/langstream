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
                CodeArchiveMetadata archiveMetadata =  new CodeArchiveMetadata(tenant, UUID.randomUUID().toString(), applicationId);
                archives.put(archiveMetadata.codeStoreId(), archiveMetadata);
                return archiveMetadata;
            }

            @Override
            public void downloadApplicationCode(String tenant, String codeStoreId, Consumer<DownloadedCodeArchive> codeArchive) throws CodeStorageException {
                codeArchive.accept(new DownloadedCodeArchive() {
                    @Override
                    public void extractTo(Path directory) throws CodeStorageException, IOException {
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
        };
    }

    @Override
    public boolean supports(String codeStorageType) {
        return "none".equals(codeStorageType);
    }
}
