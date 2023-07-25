package com.dastastax.oss.sga.kafka.runner;

import com.datastax.oss.sga.api.runner.code.Header;

record KafkaHeader(org.apache.kafka.common.header.Header header) implements Header {
    @Override
    public String key() {
        return header.key();
    }

    @Override
    public byte[] value() {
        return header.value();
    }
}
