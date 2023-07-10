package com.datastax.oss.sga.impl.storage;

import com.datastax.oss.sga.api.storage.TenantDataStore;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;

@Getter
public class InMemoryConfigStore implements TenantDataStore {

    private Map<String, String> store = new LinkedHashMap<>();

    @Override
    public boolean includeScope(Scope scope) {
        return true;
    }

    @Override
    public String storeType() {
        return "test";
    }

    @Override
    public void initialize(Map<String, String> configuration) {
    }

    @Override
    public void initializeTenant(String tenant) {
    }

    @Override
    public void put(String tenant, String key, String value) {
        store.put(key, value);
    }

    @Override
    public void delete(String tenant, String key) {
        store.remove(key);
    }

    @Override
    public String get(String tenant, String key) {
        return store.get(key);
    }

    @Override
    public LinkedHashMap<String, String> list(String tenant) {
        return store.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> b, LinkedHashMap::new));
    }
}
