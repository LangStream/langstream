package com.datastax.oss.sga.impl.storage;

import com.datastax.oss.sga.api.storage.ConfigStore;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LocalStorageConfigStore implements ConfigStore {

    protected static final String LOCAL_BASEDIR = "local.basedir";
    private String baseDir;

    @Override
    public String storeType() {
        return "local";
    }

    @Override
    public void initialize(Map<String, String> configuration) {
        this.baseDir = configuration.getOrDefault(LOCAL_BASEDIR, "sga-data");
        final Path basePath = Path.of(baseDir);
        basePath.toFile().mkdirs();
        log.info("Configured local storage at {}", baseDir);

    }

    @Override
    @SneakyThrows
    public void put(String key, String value) {
        Files.write(computePathForKey(key), value.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    @SneakyThrows
    public void delete(String key) {
        try {
            Files.delete(computePathForKey(key));
        } catch (NoSuchFileException e) {
        }
    }

    private Path computePathForKey(String key) {
        return Path.of(baseDir, key);
    }

    @Override
    @SneakyThrows
    public String get(String key) {
        try {
            return Files.readString(computePathForKey(key));
        } catch (NoSuchFileException e) {
            return null;
        }
    }

    @Override
    @SneakyThrows
    public LinkedHashMap<String, String> list() {
        return Files.list(Path.of(baseDir))
                .collect(Collectors.toMap(p -> p.toFile().getName(), p -> {
                    try {
                        return Files.readString(p);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }, (a, b) -> b, LinkedHashMap::new));
    }
}
