package com.datastax.oss.sga.api.runner.topics;

import com.datastax.oss.sga.api.runner.code.Record;

import java.util.List;

public interface TopicConsumer extends AutoCloseable {

    default Object getNativeConsumer() {
        return null;
    }

    default void start() throws Exception {}

    default void close() throws Exception  {}

    default List<Record> read()throws Exception  {
        return List.of();
    }

    default void commit(List<Record> records) throws Exception {
    }
}
