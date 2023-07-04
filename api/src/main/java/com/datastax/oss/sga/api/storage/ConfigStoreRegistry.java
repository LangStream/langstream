package com.datastax.oss.sga.api.storage;

import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;

public class ConfigStoreRegistry {

    public static ConfigStore loadConfigsStore(String type, Map<String, String> configuration) {
        return loadConfigsStore(type, ConfigStore.Scope.CONFIGS, configuration);

    }

    public static ConfigStore loadSecretsStore(String type, Map<String, String> configuration) {
        return loadConfigsStore(type, ConfigStore.Scope.SECRETS, configuration);

    }

    private static ConfigStore loadConfigsStore(String type, ConfigStore.Scope scope,
                                                Map<String, String> configuration) {
        Objects.requireNonNull(type, "type cannot be null");
        Objects.requireNonNull(configuration, "configuration cannot be null");
        ServiceLoader<ConfigStore> loader = ServiceLoader.load(ConfigStore.class);
        final ConfigStore configStore = loader
                .stream()
                .filter(p -> {
                    final ConfigStore store = p.get();
                    return type.equals(store.storeType()) && store.acceptScope(scope);
                })
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No ConfigStore found for type " + type))
                .get();
        configStore.initialize(configuration);
        return configStore;
    }

}
