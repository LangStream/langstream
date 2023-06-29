package com.datastax.oss.sga.api.model;

import java.util.HashMap;
import java.util.Map;

public record Secrets (Map<String, Secret> secrets){
    public Secrets {
        if (secrets == null) {
            secrets = new HashMap<>();
        }
    }
}
