package com.datastax.oss.sga.impl.codestorage;

import com.datastax.oss.sga.api.codestorage.CodeStorage;
import com.datastax.oss.sga.api.codestorage.CodeStorageProvider;
import lombok.extern.slf4j.Slf4j;
import java.nio.file.Paths;
import java.util.Map;

@Slf4j
public class LocalDiskCodeStorageProvider implements CodeStorageProvider {

    public static final String PATH = "path";
    @Override
    public CodeStorage createImplementation(String codeStorageType, Map<String, Object> configuration) {
        String path = configuration.getOrDefault(PATH, "/tmp").toString();
        return new LocalDiskCodeStorage(Paths.get(path));
    }

    @Override
    public boolean supports(String codeStorageType) {
        return "local".equals(codeStorageType);
    }
}
