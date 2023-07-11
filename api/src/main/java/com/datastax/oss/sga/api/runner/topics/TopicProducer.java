package com.datastax.oss.sga.api.runner.topics;

import com.datastax.oss.sga.api.runner.code.Record;

import java.util.List;

public interface TopicProducer {

    default void start() {}

    default void close() {}

    default void write(List<Record> records) {}
}
