package com.datastax.oss.sga.api.runner.code;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Collection;
import java.util.Set;

/**
 * Basic implementation of {@link Record} interface.
 */
@Data
@Builder
public final class SimpleRecord implements Record {
    final Object key;
    final Object value;
    final String origin;
    final Long timestamp;
    @Builder.Default
    final Collection<Header> headers = Set.of();

    @Override
    public Object key() {
        return key;
    }

    @Override
    public Object value() {
        return value;
    }

    @Override
    public String origin() {
        return origin;
    }

    @Override
    public Long timestamp() {
        return timestamp;
    }

    @Override
    public Collection<Header> headers() {
        return headers;
    }
}
