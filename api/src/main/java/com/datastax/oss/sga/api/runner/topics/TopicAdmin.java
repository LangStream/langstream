package com.datastax.oss.sga.api.runner.topics;

public interface TopicAdmin extends AutoCloseable {
    default void start() {}

    default void close() {}

    default Object getNativeTopicAdmin() {
        return null;
    }

}
