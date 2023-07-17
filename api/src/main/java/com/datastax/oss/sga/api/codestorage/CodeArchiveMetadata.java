package com.datastax.oss.sga.api.codestorage;

import lombok.Data;

import java.util.Objects;


public record CodeArchiveMetadata (
    String tenant,
    String codeStoreId,
    String applicationId) {
    public CodeArchiveMetadata {
        Objects.requireNonNull(tenant);
        Objects.requireNonNull(codeStoreId);
        Objects.requireNonNull(applicationId);
    }
}
