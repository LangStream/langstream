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
package com.datastax.oss.sga.api.codestorage;

import com.datastax.oss.sga.api.model.ComputeCluster;
import com.datastax.oss.sga.api.model.StreamingCluster;
import com.datastax.oss.sga.api.runtime.ComputeClusterRuntime;
import com.datastax.oss.sga.api.runtime.ComputeClusterRuntimeProvider;
import com.datastax.oss.sga.api.runtime.StreamingClusterRuntime;
import com.datastax.oss.sga.api.runtime.StreamingClusterRuntimeProvider;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This is the API to load a CodeStorage implementation.
 */
@Slf4j
public class CodeStorageRegistry {

    public static CodeStorage getCodeStorage(String codeStorageType, Map<String, Object> configuration) {
        log.info("Loading CodeStorage implementation for type {} with configuration {}", codeStorageType, configuration);
        Objects.requireNonNull(codeStorageType, "codeStorageType cannot be null");

        ServiceLoader<CodeStorageProvider> loader = ServiceLoader.load(CodeStorageProvider.class);
        ServiceLoader.Provider<CodeStorageProvider> codeStorageProvider = loader
                .stream()
                .filter(p -> {
                    return p.get().supports(codeStorageType);
                })
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No CodeStorage found for type " + codeStorageType));

        final CodeStorage implementation = codeStorageProvider.get().createImplementation(codeStorageType, configuration);
        return implementation;
    }


}
