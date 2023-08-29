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
package ai.langstream.api.util;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility class for configuration related operations. This class is widely used and changing the
 * behaviour may cause breaking changes.
 */
public class ConfigurationUtils {

    /**
     * Decode a configuration map into a list of strings. Nulls are removed
     *
     * @param key the entry
     * @param configuration the agent configuration
     * @return a list of strings
     */
    public static List<String> getList(String key, Map<String, Object> configuration) {
        Object value = configuration.get(key);
        if (value == null) {
            return List.of();
        }
        if (value instanceof String s) {
            return List.of(s.split(",")).stream().map(String::trim).collect(Collectors.toList());
        } else if (value instanceof Collection) {
            return ((Collection<String>) value)
                    .stream()
                            .filter(v -> v != null)
                            .map(v -> v.toString())
                            .map(String::trim)
                            .collect(Collectors.toList());
        } else {
            throw new IllegalArgumentException(
                    "Unsupported type for " + key + ": " + value.getClass());
        }
    }

    /**
     * Decode a configuration map into a set of unique and unordered strings.
     *
     * @param key the entry
     * @param configuration the agent configuration
     * @return a set of unique strings
     */
    public static Set<String> getSet(String key, Map<String, Object> configuration) {
        return Collections.unmodifiableSet(new HashSet<>(getList(key, configuration)));
    }

    public static int getInt(String key, int defaultValue, Map<String, Object> configuration) {
        Object value = configuration.getOrDefault(key, defaultValue);
        if (value instanceof Number n) {
            return n.intValue();
        } else {
            return Integer.parseInt(value.toString());
        }
    }

    public static Integer getInteger(
            String key, Integer defaultValue, Map<String, Object> configuration) {
        Object value = configuration.getOrDefault(key, defaultValue);
        if (value == null) {
            return null;
        }
        if (value instanceof Number n) {
            return n.intValue();
        } else {
            return Integer.parseInt(value.toString());
        }
    }

    public static Double getDouble(
            String key, Double defaultValue, Map<String, Object> configuration) {
        Object value = configuration.getOrDefault(key, defaultValue);
        if (value == null) {
            return null;
        }
        if (value instanceof Number n) {
            return n.doubleValue();
        } else {
            return Double.parseDouble(value.toString());
        }
    }

    public static boolean getBoolean(
            String key, boolean defaultValue, Map<String, Object> configuration) {
        Object value = configuration.getOrDefault(key, defaultValue);
        if (value instanceof Boolean n) {
            return n.booleanValue();
        } else {
            return Boolean.parseBoolean(value.toString());
        }
    }

    public static String getString(
            String key, String defaultValue, Map<String, Object> configuration) {
        Object value = configuration.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof String n) {
            return n;
        } else {
            return value.toString();
        }
    }
}
