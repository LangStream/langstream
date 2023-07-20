package com.datastax.oss.sga.api.model;

import java.util.Objects;
import java.util.Set;

public record Dependency(String name, String url, String type, String sha512sum) {

    public static final Set<String> SUPPORTED_TYPES = Set.of("java-library");

    public Dependency {
        if (type == null || !SUPPORTED_TYPES.contains(type)) {
            throw new IllegalArgumentException("Unsupported type: " + type + ", only " + SUPPORTED_TYPES);
        }
        Objects.requireNonNull(name, "name cannot be null");
        Objects.requireNonNull(url, "url cannot be null");
        Objects.requireNonNull(sha512sum, "sha512sum cannot be null");
    }
}
