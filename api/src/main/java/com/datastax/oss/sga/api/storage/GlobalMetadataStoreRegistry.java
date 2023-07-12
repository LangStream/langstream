package com.datastax.oss.sga.api.storage;

import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;

public class GlobalMetadataStoreRegistry {

    public static GlobalMetadataStore loadStore(String type, Map<String, Object> configuration) {
        Objects.requireNonNull(type, "type cannot be null");
        Objects.requireNonNull(configuration, "configuration cannot be null");
        ServiceLoader<GlobalMetadataStore> loader = ServiceLoader.load(GlobalMetadataStore.class);
        final GlobalMetadataStore store = loader
                .stream()
                .filter(p -> {
                    return type.equals(p.get().storeType());
                })
                .findFirst()
                .orElseThrow(
                        () -> new RuntimeException("No GlobalMetadataStore found for type " + type)
                )
                .get();
        store.initialize(configuration);
        return store;
    }

}
