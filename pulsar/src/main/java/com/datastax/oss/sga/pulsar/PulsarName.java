package com.datastax.oss.sga.pulsar;

public record PulsarName(String tenant, String namespace, String name) {
    public String toPulsarName() {
        return tenant + "/" + namespace + "/" + name;
    }
}
