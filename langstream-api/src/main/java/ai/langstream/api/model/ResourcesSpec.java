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
package ai.langstream.api.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonDeserialize(using = ResourcesSpecDeserializer.class) // Use custom deserializer
public record ResourcesSpec(Object parallelism, Object size, DiskSpec disk) {

    public static final ResourcesSpec DEFAULT = new ResourcesSpec(1, 1, null);

    // withDefaultsFrom method
    public ResourcesSpec withDefaultsFrom(ResourcesSpec higherLevel) {
        if (higherLevel == null) {
            return this;
        }
        Object newParallelism = parallelism == null ? higherLevel.parallelism() : parallelism;
        Object newSize = size == null ? higherLevel.size() : size;
        DiskSpec newDisk = disk == null ? higherLevel.disk() : disk;

        return new ResourcesSpec(newParallelism, newSize, newDisk);
    }

    // resolveVariables method
    public ResourcesSpec resolveVariables(Map<String, Integer> variableMap) {
        Integer resolvedParallelism = resolveToObject(parallelism, variableMap);
        Integer resolvedSize = resolveToObject(size, variableMap);
        return new ResourcesSpec(resolvedParallelism, resolvedSize, disk);
    }

    // Helper method to resolve objects or strings to Integer
    private static Integer resolveToObject(Object value, Map<String, Integer> variableMap) {
        if (value instanceof String) {
            String stringValue = (String) value;
            if (stringValue.startsWith("${") && stringValue.endsWith("}")) {
                String variableName = stringValue.substring(2, stringValue.length() - 1);
                return variableMap.getOrDefault(variableName, null);
            }
            try {
                return Integer.parseInt(stringValue);
            } catch (NumberFormatException e) {
                System.err.println("Error parsing integer: " + e.getMessage());
                return null;
            }
        } else if (value instanceof Integer) {
            return (Integer) value;
        }
        return null;
    }
}
