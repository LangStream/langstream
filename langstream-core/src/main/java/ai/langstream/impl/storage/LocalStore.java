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
package ai.langstream.impl.storage;

import ai.langstream.api.storage.GlobalMetadataStore;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LocalStore implements GlobalMetadataStore {

    public static final String LOCAL_BASEDIR = "local.basedir";
    protected static final String GLOBAL_DIR = "__global_";
    private String baseDir;

    @Override
    public String storeType() {
        return "local";
    }

    @Override
    public void initialize(Map<String, Object> configuration) {
        this.baseDir = configuration.getOrDefault(LOCAL_BASEDIR, "langstream-data").toString();
        final Path basePath = Path.of(baseDir);
        basePath.toFile().mkdirs();
        Path.of(baseDir, GLOBAL_DIR).toFile().mkdirs();
        log.info("Configured local storage at {}", baseDir);
    }

    @SneakyThrows
    private void write(Path path, String value) {
        Files.writeString(path, value);
    }

    @SneakyThrows
    private void delete(Path path) {
        try {
            Files.delete(path);
        } catch (NoSuchFileException e) {
            // ignore
        }
    }

    private Path computePathForKey(String key) {
        return Path.of(baseDir, GLOBAL_DIR, key);
    }

    @SneakyThrows
    private String get(Path path) {
        try {
            return Files.readString(path);
        } catch (NoSuchFileException e) {
            return null;
        }
    }

    @SneakyThrows
    private LinkedHashMap<String, String> listFromPath(Path path) {
        return Files.list(path)
                .collect(
                        Collectors.toMap(
                                p -> p.toFile().getName(),
                                p -> {
                                    try {
                                        return Files.readString(p);
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                },
                                (a, b) -> b,
                                LinkedHashMap::new));
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
