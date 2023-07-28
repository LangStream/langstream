package com.datastax.oss.sga.api.runner.topics;


public interface TopicReader extends AutoCloseable {

    default void start() throws Exception {}

    default void close() throws Exception  {}

    TopicReadResult read() throws Exception;
}
