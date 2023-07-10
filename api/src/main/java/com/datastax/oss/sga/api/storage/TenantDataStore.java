package com.datastax.oss.sga.api.storage;

import java.util.LinkedHashMap;

public interface TenantDataStore extends GenericStore {

    enum Scope {
        APPS,
        SECRETS
    }

    boolean includeScope(Scope scope);

    void initializeTenant(String tenant);

    void put(String tenant, String key, String value);

    void delete(String tenant, String key);

    String get(String tenant, String key);

    LinkedHashMap<String, String> list(String tenant);

}
