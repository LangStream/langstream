package com.datastax.oss.sga.api.runtime;

public interface Topic extends ConnectionImplementation {
    String topicName();
    boolean implicit();
}
