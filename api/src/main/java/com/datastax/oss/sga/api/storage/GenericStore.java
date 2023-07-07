package com.datastax.oss.sga.api.storage;

import java.util.Map;

public interface GenericStore {
    String storeType();

    void initialize(Map<String, String> configuration);
}
