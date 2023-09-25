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
package ai.langstream.tests.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SystemOrEnv {

    public static Map<String, String> getProperties(String envPrefix, String sysPrefix) {
        Map<String, String> result = new HashMap<>();
        final Map<String, String> envs = System.getenv();
        for (Map.Entry<String, String> env : envs.entrySet()) {
            if (env.getKey().startsWith(envPrefix)) {
                final String key = env.getKey().substring(envPrefix.length()).toLowerCase();
                final String value = env.getValue();
                result.put(key, value);
            }
        }

        final Set<Object> props = System.getProperties().keySet();
        for (Object prop : props) {
            String asString = prop.toString();
            if (asString.startsWith(sysPrefix)) {
                final String key = asString.substring(sysPrefix.length());
                final String value = System.getProperty(asString);
                result.put(key, value);
            }
        }
        return result;
    }

    public static String getProperty(String envProperty, String sysProperty) {
        return getProperty(envProperty, sysProperty, null);
    }

    public static String getProperty(String envProperty, String sysProperty, String defaultValue) {
        String value = System.getProperty(sysProperty);
        if (value == null) {
            value = System.getenv(envProperty);
        }
        if (value == null) {
            value = defaultValue;
        }
        return value;
    }
}
