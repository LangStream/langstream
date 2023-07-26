package com.dastastax.oss.sga.kafka.runner;

import com.datastax.oss.sga.api.runner.code.Header;
import java.nio.charset.StandardCharsets;

record KafkaHeader(org.apache.kafka.common.header.Header header) implements Header {
    @Override
    public String key() {
        return header.key();
    }

    @Override
    public byte[] value() {
        return header.value();
    }

    @Override
    public String valueAsString() {
        if (header.value() == null) {
            return null;
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
