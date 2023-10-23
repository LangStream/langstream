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
package ai.langstream.webservice.archetype;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ArchetypeStore {

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    private final Map<String, ArchetypeDefinition> archetypeDefinitions = new HashMap<>();

    public void load(String path) throws Exception {
        Path directory = Paths.get(path);
        log.info("Loading archetypes from {}", directory);
        if (Files.isDirectory(directory)) {
            directory = directory.toAbsolutePath();
            try (var stream = Files.newDirectoryStream(directory)) {
                stream.forEach(
                        file -> {
                            log.info("Loading archetype from {}", file);
                            try {
                                if (Files.isDirectory(file)) {
                                    loadArchetype(file);
                                }
                            } catch (Exception e) {
                                log.error("Failed to load archetype from {}", file, e);
                                throw new RuntimeException(e);
                            }
                        });
            }
        }
    }

    private void loadArchetype(Path file) throws Exception {
        Path archetypeFile = file.resolve("archetype.yaml");
        if (Files.isRegularFile(archetypeFile)) {
            ArchetypeDefinition archetypeDefinition =
                    mapper.readValue(archetypeFile.toFile(), ArchetypeDefinition.class);
            archetypeDefinitions.put(archetypeDefinition.archetype().id(), archetypeDefinition);
        } else {
            throw new IllegalArgumentException("Archetype file not found: " + archetypeFile);
        }
    }

    public List<String> list() {
        return new ArrayList<>(archetypeDefinitions.keySet());
    }

    public ArchetypeDefinition get(String archetypeId) {
        return archetypeDefinitions.get(archetypeId);
    }
}
