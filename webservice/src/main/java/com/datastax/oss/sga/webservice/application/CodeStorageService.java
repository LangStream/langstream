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
package com.datastax.oss.sga.webservice.application;

import com.datastax.oss.sga.api.codestorage.CodeArchiveMetadata;
import com.datastax.oss.sga.api.codestorage.CodeStorage;
import com.datastax.oss.sga.api.codestorage.CodeStorageException;
import com.datastax.oss.sga.api.codestorage.CodeStorageRegistry;
import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.impl.codestorage.LocalFileUploadableCodeArchive;
import com.datastax.oss.sga.webservice.config.StorageProperties;
import lombok.extern.jbosslog.JBossLog;
import org.springframework.stereotype.Service;

import java.nio.file.Path;
import java.util.Objects;
import java.util.UUID;

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
