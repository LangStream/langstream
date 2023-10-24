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
package ai.langstream.api.archetype;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record ArchetypeDefinition(Archetype archetype) {

    public record Archetype(
            String id,
            String title,
            List<String> labels,
            String description,
            String icon,
            List<Section> sections) {}

    public record Section(String title, String description, List<Parameter> parameters) {}

    public record Parameter(
            String name,
            String label,
            String description,
            String type,
            String subtype,
            String binding,
            boolean required,
            @JsonProperty("default") Object defaultVal) {}
}
