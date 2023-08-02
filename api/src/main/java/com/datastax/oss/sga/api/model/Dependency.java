/**
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
package com.datastax.oss.sga.api.model;

import java.util.Objects;
import java.util.Set;

public record Dependency(String name, String url, String type, String sha512sum) {

    public static final Set<String> SUPPORTED_TYPES = Set.of("java-library");

    public Dependency {
        if (type == null || !SUPPORTED_TYPES.contains(type)) {
            throw new IllegalArgumentException("Unsupported type: " + type + ", only " + SUPPORTED_TYPES);
        }
        Objects.requireNonNull(name, "name cannot be null");
        Objects.requireNonNull(url, "url cannot be null");
        Objects.requireNonNull(sha512sum, "sha512sum cannot be null");
    }
}
