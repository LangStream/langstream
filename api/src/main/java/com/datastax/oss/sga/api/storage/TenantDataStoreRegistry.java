package com.datastax.oss.sga.api.storage;

import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;

public class TenantDataStoreRegistry {

    public static TenantDataStore loadAppsStore(String type, Map<String, String> configuration) {
        return loadStore(type, TenantDataStore.Scope.APPS, configuration);

    }

    public static TenantDataStore loadSecretsStore(String type, Map<String, String> configuration) {
        return loadStore(type, TenantDataStore.Scope.SECRETS, configuration);

    }

    private static TenantDataStore loadStore(String type, TenantDataStore.Scope scope,
                                             Map<String, String> configuration) {
        Objects.requireNonNull(type, "type cannot be null");
        Objects.requireNonNull(configuration, "configuration cannot be null");
        ServiceLoader<TenantDataStore> loader = ServiceLoader.load(TenantDataStore.class);
        final TenantDataStore configStore = loader
                .stream()
                .filter(p -> {
                    final TenantDataStore store = p.get();
                    return type.equals(store.storeType()) && store.includeScope(scope);
                })
                .findFirst()
                .orElseThrow(
                        () -> new RuntimeException("No TenantDataStore found for type " + type + " scope " + scope))
                .get();
        configStore.initialize(configuration);
        return configStore;
    }

}
