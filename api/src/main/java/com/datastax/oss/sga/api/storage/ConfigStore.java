package com.datastax.oss.sga.api.storage;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public interface ConfigStore {

    String storeType();

    void initialize(Map<String, String> configuration);

    void put(String key, String value);

    void delete(String key);

    String get(String key);

    LinkedHashMap<String, String> list();

}
