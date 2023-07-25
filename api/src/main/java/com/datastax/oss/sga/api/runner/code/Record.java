package com.datastax.oss.sga.api.runner.code;

import java.util.Collection;
public interface Record {
    Object key();
    Object value();
    String origin();
    Long timestamp();
    Collection<Header> headers();

    default Header getHeader(String key) {
        return headers()
                .stream()
                .filter(h -> h.key().equals(key)).findFirst().orElse(null);
    }
}
