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

import java.net.URL;
import java.net.URLClassLoader;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Utility class for classloader related operations. */
public class ClassloaderUtils {

    public static class ClassloaderContext implements AutoCloseable {
        private final ClassLoader previous;

        private ClassloaderContext(ClassLoader newContextClassLoader) {
            this.previous = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(newContextClassLoader);
        }

        public void close() {
            if (previous != null) {
                Thread.currentThread().setContextClassLoader(previous);
            }
        }
    }

    public static ClassloaderContext withContextClassloader(ClassLoader cl) {
        return new ClassloaderContext(cl);
    }

    public static <T> T executeWithClassloader(
            ClassLoader cl, java.util.function.Supplier<T> supplier) {
        try (ClassloaderContext ignored = withContextClassloader(cl)) {
            return supplier.get();
        }
    }

    public static String describeClassloader(ClassLoader classLoader) {
        StringBuilder result = new StringBuilder();
        if (classLoader instanceof URLClassLoader urlClassLoader) {
            result.append(classLoader).append(": {");
            URL[] urLs = urlClassLoader.getURLs();
            if (urLs != null) {
                String urlsString =
                        Stream.of(urLs).map(String::valueOf).collect(Collectors.joining(",\n"));
                result.append("urls: ").append(urlsString);
            }
            result.append("}");
        } else {
            result.append(classLoader);
        }
        if (classLoader.getParent() != null) {
            result.append("\n Parent: ")
                    .append(describeClassloader(classLoader.getParent()))
                    .append("\n");
        }
        return result.toString();
    }
}
