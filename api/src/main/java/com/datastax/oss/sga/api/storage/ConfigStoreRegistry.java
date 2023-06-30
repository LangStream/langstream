package com.datastax.oss.sga.api.storage;

import com.datastax.oss.sga.api.runtime.ClusterRuntime;
import com.datastax.oss.sga.api.runtime.ClusterRuntimeProvider;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;

public class ConfigStoreRegistry {

    public static ConfigStore loadConfigStore(String type, Map<String, String> configuration) {
        Objects.requireNonNull(type, "type cannot be null");
        Objects.requireNonNull(configuration, "configuration cannot be null");
        ServiceLoader<ConfigStore> loader = ServiceLoader.load(ConfigStore.class);
        final ConfigStore configStore = loader
                .stream()
                .filter(p -> {
                    return type.equals(p.get().storeType());
                })
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No ConfigStore found for type " + type))
                .get();
        configStore.initialize(configuration);
        return configStore;
    }

}
