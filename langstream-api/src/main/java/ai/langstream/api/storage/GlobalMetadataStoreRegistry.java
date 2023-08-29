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
package ai.langstream.api.storage;

import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;

public class GlobalMetadataStoreRegistry {

    public static GlobalMetadataStore loadStore(String type, Map<String, Object> configuration) {
        Objects.requireNonNull(type, "type cannot be null");
        Objects.requireNonNull(configuration, "configuration cannot be null");
        ServiceLoader<GlobalMetadataStore> loader = ServiceLoader.load(GlobalMetadataStore.class);
        final GlobalMetadataStore store =
                loader.stream()
                        .filter(p -> type.equals(p.get().storeType()))
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new RuntimeException(
                                                "No GlobalMetadataStore found for type " + type))
                        .get();
        store.initialize(configuration);
        return store;
    }
}
