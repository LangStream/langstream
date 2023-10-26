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
import java.util.List;

public record ArchetypeBasicInfo(
        String id, String title, List<String> labels, String description, String icon) {

    public static ArchetypeBasicInfo fromArchetypeDefinition(ArchetypeDefinition definition) {
        return new ArchetypeBasicInfo(
                definition.archetype().id(),
                definition.archetype().title(),
                definition.archetype().labels(),
                definition.archetype().description(),
                definition.archetype().icon());
    }
}
