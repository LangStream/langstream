package com.datastax.oss.sga.api.runner.code;

import lombok.AllArgsConstructor;
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

    /**
     * Copy all fields from {@link Record} to new {@link SimpleRecordBuilder}.
     * @param record
     * @return
     */
    public static SimpleRecordBuilder copyFrom(Record record) {
        return builder()
                .key(record.key())
                .value(record.value())
                .origin(record.origin())
                .headers(record.headers())
                .timestamp(record.timestamp());
    }

    @Data
    @AllArgsConstructor
    public static class SimpleHeader implements Header {

        public static SimpleHeader of(String key, String value) {
            return new SimpleHeader(key, value);
        }

        final String key;
        final String value;

        @Override
        public String key() {
            return key;
        }

        @Override
        public String value() {
            return value;
        }

        @Override
        public String valueAsString() {
            return value;
        }
    }
}
