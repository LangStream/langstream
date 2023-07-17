package com.datastax.oss.sga.webservice.application;

import com.datastax.oss.sga.api.codestorage.CodeArchiveMetadata;
import com.datastax.oss.sga.api.codestorage.CodeStorage;
import com.datastax.oss.sga.api.codestorage.CodeStorageException;
import com.datastax.oss.sga.api.codestorage.CodeStorageRegistry;
import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.impl.codestorage.LocalFileUploadableCodeArchive;
import com.datastax.oss.sga.webservice.config.StorageProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.nio.file.Path;
import java.util.Objects;
import java.util.UUID;

@Service
public class CodeStorageService {

    private final CodeStorage codeStorage;

    public CodeStorageService(StorageProperties storageProperties) {
        codeStorage =
                CodeStorageRegistry.getCodeStorage(storageProperties.getCodeStorage().getType(),
                        storageProperties.getCodeStorage().getConfiguration());
    }

    public CodeStorage getCodeStorage() {
        return codeStorage;
    }

    public String deployApplicationCodeStorage(String tenant, String application,
                                               Application applicationInstance, Path zipFile) throws CodeStorageException {
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

}
