package com.datastax.oss.sga.api.model;

public record SchemaDefinition(String type, String schema, String name) {
    public SchemaDefinition {
        if (type == null || type.isEmpty()) {
            throw new IllegalArgumentException("type cannot be empty");
        }
        if (name == null) {
            name = "Schema";
        }
    }
}
