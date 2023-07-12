package com.datastax.oss.sga.api.storage;

import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;

public class ApplicationStoreRegistry {

    public static ApplicationStore loadStore(String type, Map<String, Object> configuration) {
        Objects.requireNonNull(type, "type cannot be null");
        Objects.requireNonNull(configuration, "configuration cannot be null");
        ServiceLoader<ApplicationStore> loader = ServiceLoader.load(ApplicationStore.class);
        final ApplicationStore store = loader
                .stream()
                .filter(p -> {
                    return type.equals(p.get().storeType());
                })
                .findFirst()
                .orElseThrow(
                        () -> new RuntimeException("No ApplicationStore found for type " + type)
                )
                .get();
        store.initialize(configuration);
        return store;
    }

}
