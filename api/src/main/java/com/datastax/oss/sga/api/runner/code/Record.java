package com.datastax.oss.sga.api.runner.code;

public interface Record {
    Object key();
    Object value();
    String origin();
}
