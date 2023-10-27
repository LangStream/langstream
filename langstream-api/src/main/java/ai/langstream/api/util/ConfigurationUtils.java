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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility class for configuration related operations. This class is widely used and changing the
 * behaviour may cause breaking changes.
 */
@Slf4j
public class ConfigurationUtils {

    private static boolean DEVELOPMENT_MODE =
            Boolean.parseBoolean(System.getProperty("langstream.development.mode", "false"));

    static {
        if (DEVELOPMENT_MODE) {
            log.warn("Development mode is enabled, this should not be used in production");
        }
    }

    public static boolean isDevelopmentMode() {
        return DEVELOPMENT_MODE;
    }

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
            return defaultValue;
        }
        if (value instanceof Number n) {
            return n.intValue();
        } else {
            return Integer.parseInt(value.toString());
        }
    }

    public static Long getLong(String key, Long defaultValue, Map<String, Object> configuration) {
        Object value = configuration.getOrDefault(key, defaultValue);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number n) {
            return n.longValue();
        } else {
            return Long.parseLong(value.toString());
        }
    }

    public static Double getDouble(
            String key, Double defaultValue, Map<String, Object> configuration) {
        Object value = configuration.getOrDefault(key, defaultValue);
        if (value == null) {
            return defaultValue;
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
        if (value == null) {
            return defaultValue;
        }
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

    public static Map<String, Object> getMap(
            String key, Map<String, Object> defaultValue, Map<String, Object> configuration) {
        Object value = configuration.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Map map) {
            return (Map<String, Object>) Collections.unmodifiableMap(map);
        }
        throw new IllegalArgumentException(
                "Unsupported type for "
                        + key
                        + ", expecting a Map, got got a "
                        + value.getClass().getName());
    }

    public static <T> T requiredField(
            Map<String, Object> configuration, String name, Supplier<String> definition) {
        Object value = configuration.get(name);
        if (value == null) {
            throw new IllegalArgumentException(
                    "Missing required field '" + name + "' in " + definition.get());
        }
        return (T) value;
    }

    public static String requiredNonEmptyField(
            Map<String, Object> configuration, String name, Supplier<String> definition) {
        Object value = configuration.get(name);
        if (value == null || value.toString().isEmpty()) {
            throw new IllegalArgumentException(
                    "Missing required field '" + name + "' in " + definition.get());
        }
        return value.toString();
    }

    public static void validateEnumField(
            Map<String, Object> configuration,
            String name,
            Set<String> values,
            Supplier<String> definition) {
        Object value = configuration.get(name);
        validateEnumValue(name, values, value, definition);
    }

    public static void validateEnumValue(
            String name, Set<String> values, Object value, Supplier<String> definition) {
        if (value == null || value.toString().isEmpty()) {
            return;
        }
        if (!values.contains(value.toString())) {
            throw new IllegalArgumentException(
                    "Value "
                            + value
                            + " is not allowed for field '"
                            + name
                            + ", only "
                            + values
                            + " are allowed', in "
                            + definition.get());
        }
    }

    public static void validateInteger(
            Map<String, Object> configuration,
            String name,
            int min,
            int max,
            Supplier<String> definition) {
        Object value = configuration.get(name);
        if (value == null || value.toString().isEmpty()) {
            return;
        }
        int number;
        if (value instanceof Number) {
            number = ((Number) value).intValue();
        } else {
            number = Integer.parseInt(value.toString());
        }
        if (number < min) {
            throw new IllegalArgumentException(
                    "Integer field '"
                            + name
                            + "' is "
                            + number
                            + ", but it must be greater or equals to "
                            + min
                            + ", in "
                            + definition.get());
        }
        if (number > max) {
            throw new IllegalArgumentException(
                    "Integer field '"
                            + name
                            + "' is "
                            + number
                            + ", but it must be less or equals to "
                            + max
                            + ", in "
                            + definition.get());
        }
    }

    public static void requiredListField(
            Map<String, Object> configuration, String name, Supplier<String> definition) {
        Object value = configuration.get(name);
        if (value == null) {
            throw new IllegalArgumentException(
                    "Missing required field '" + name + "' in " + definition.get());
        }

        if (!(value instanceof List)) {
            throw new IllegalArgumentException(
                    "Expecting a list in the field '" + name + "' in " + definition.get());
        }
    }

    /**
     * Remove all the secrets from the configuration. This method is used to avoid logging secrets
     *
     * @param object
     * @return the object without secrets
     */
    public static Object redactSecrets(Object object) {
        if (object == null) {
            return null;
        }

        if (object instanceof List list) {
            List<Object> other = new ArrayList<>(list.size());
            list.forEach(o -> other.add(redactSecrets(o)));
            return other;
        }
        if (object instanceof Set set) {
            Set<Object> other = new HashSet<>(set.size());
            set.forEach(o -> other.add(redactSecrets(o)));
            return other;
        }

        if (object instanceof Map map) {
            Map<Object, Object> other = new HashMap<>();
            map.forEach(
                    (k, v) -> {
                        String keyLowercase = (String.valueOf(k)).toLowerCase();
                        if (keyLowercase.contains("password")
                                || keyLowercase.contains("pwd")
                                || keyLowercase.contains("secure")
                                || keyLowercase.contains("secret")
                                || keyLowercase.contains("serviceaccountjson")
                                || keyLowercase.contains("access-key")
                                || keyLowercase.contains("token")) {
                            other.put(k, "<REDACTED>");
                        } else {
                            other.put(k, redactSecrets(v));
                        }
                    });
            return other;
        }

        return object;
    }
}
