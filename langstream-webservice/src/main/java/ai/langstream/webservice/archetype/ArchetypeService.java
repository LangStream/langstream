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

import ai.langstream.api.archetype.ArchetypeDefinition;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@AllArgsConstructor
public class ArchetypeService {

    private final ArchetypeStore archetypeStore;

    public ArchetypeDefinition getArchetype(String tenant, String id) {
        return archetypeStore.get(id);
    }

    public List<ArchetypeBasicInfo> getAllArchetypesBasicInfo(String tenant) {
        return archetypeStore.list().stream()
                .map(id -> ArchetypeBasicInfo.fromArchetypeDefinition(archetypeStore.get(id)))
                .collect(Collectors.toList());
    }

    public Path getArchetypePath(String archetypeId) {
        return archetypeStore.getArchetypePath(archetypeId);
    }

    public Path buildArchetypeZip(String archetypeId) throws Exception {
        return archetypeStore.buildArchetypeZip(archetypeId);
    }
}
