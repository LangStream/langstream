package com.datastax.oss.sga.impl.storage.k8s.codestorage;

import com.datastax.oss.sga.api.codestorage.CodeStorage;
import com.datastax.oss.sga.api.codestorage.CodeStorageProvider;
import lombok.extern.slf4j.Slf4j;
import java.util.Map;

@Slf4j
public class S3CodeStorageProvider implements CodeStorageProvider {

    @Override
    public CodeStorage createImplementation(String codeStorageType, Map<String, Object> configuration) {
        return new S3CodeStorage(configuration);
    }

    @Override
    public boolean supports(String codeStorageType) {
        return "s3".equals(codeStorageType);
    }
}
