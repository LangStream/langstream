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
package ai.langstream.api.doc;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AgentConfigurationModel {

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class AgentConfigurationProperty {
        private String description;
        boolean required;
        private String type;
        private Map<String, AgentConfigurationProperty> properties;
        private AgentConfigurationProperty items;
        private Object defaultValue;
    }

    private String name;
    private String description;
    private Map<String, AgentConfigurationProperty> properties;
}
