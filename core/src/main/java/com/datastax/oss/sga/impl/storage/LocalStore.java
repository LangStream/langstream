package com.datastax.oss.sga.impl.storage;

import com.datastax.oss.sga.api.storage.GlobalMetadataStore;
import com.datastax.oss.sga.api.storage.TenantDataStore;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LocalStore implements TenantDataStore, GlobalMetadataStore {

    public static final String LOCAL_BASEDIR = "local.basedir";
    protected static final String GLOBAL_DIR = "__global_";
    private String baseDir;

    @Override
    public String storeType() {
        return "local";
    }

    @Override
    public boolean includeScope(Scope scope) {
        return true;
    }

    @Override
    public void initialize(Map<String, String> configuration) {
        this.baseDir = configuration.getOrDefault(LOCAL_BASEDIR, "sga-data");
        final Path basePath = Path.of(baseDir);
        basePath.toFile().mkdirs();
        Path.of(baseDir, GLOBAL_DIR).toFile().mkdirs();
        log.info("Configured local storage at {}", baseDir);
    }

    @Override
    public void initializeTenant(String tenant) {
        Path.of(baseDir, tenant).toFile().mkdirs();
    }

    @Override
    @SneakyThrows
    public void put(String tenant, String key, String value) {
        write(computePathForKey(tenant, key), value);
    }

    @SneakyThrows
    private void write(Path path, String value) {
        Files.write(path, value.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    @SneakyThrows
    public void delete(String tenant, String key) {
        delete(computePathForKey(tenant, key));
    }

    @SneakyThrows
    private void delete(Path path) {
        try {
            Files.delete(path);
        } catch (NoSuchFileException e) {
        }
    }

    private Path computePathForKey(String key) {
        return Path.of(baseDir, GLOBAL_DIR, key);
    }

    private Path computePathForKey(String tenant, String key) {
        return Path.of(baseDir, tenant, key);
    }

    private Path computeTenantPath(String tenant) {
        return Path.of(baseDir, tenant);
    }

    @Override
    @SneakyThrows
    public String get(String tenant, String key) {
        final Path path = computePathForKey(tenant, key);
        return get(path);
    }

    @SneakyThrows
    private String get(Path path) {
        try {
            return Files.readString(path);
        } catch (NoSuchFileException e) {
            return null;
        }
    }

    @Override
    @SneakyThrows
    public LinkedHashMap<String, String> list(String tenant) {
        final Path path = computeTenantPath(tenant);
        return listFromPath(path);
    }

    @SneakyThrows
    private LinkedHashMap<String, String> listFromPath(Path path) {
        return Files.list(path)
                .collect(Collectors.toMap(p -> p.toFile().getName(), p -> {
                    try {
                        return Files.readString(p);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }, (a, b) -> b, LinkedHashMap::new));
    }

    @Override
    public void put(String key, String value) {
        write(computePathForKey(key), value);
    }

    @Override
    public void delete(String key) {
        delete(computePathForKey(key));
    }

    @Override
    public String get(String key) {
        return get(computePathForKey(key));
    }

    @Override
    public LinkedHashMap<String, String> list() {
        return listFromPath(Path.of(baseDir, GLOBAL_DIR));
    }
}
