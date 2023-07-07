package com.datastax.oss.sga.api.storage;

import java.util.LinkedHashMap;

public interface GlobalMetadataStore extends GenericStore {

    void put(String key, String value);

    void delete(String key);

    String get(String key);

    LinkedHashMap<String, String> list();

}
