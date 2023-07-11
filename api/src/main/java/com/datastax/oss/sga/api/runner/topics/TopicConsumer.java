package com.datastax.oss.sga.api.runner.topics;

import com.datastax.oss.sga.api.runner.code.Record;

import java.util.List;

public interface TopicConsumer {

    default void start() {}

    default void close() {}

    default List<Record> read() {
        return List.of();
    }
}
